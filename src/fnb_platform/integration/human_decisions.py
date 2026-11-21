"""Human Decision Import V1. Immutable history and effective links; no Gold/merge."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

from .. import er_category_review_export as review, google_silver as silver, publication
from ..publication import publication_lock
from ..overture_incremental import _digest

VERSION = "human-decision-import-v1"
KEY = review.KEY
GROUPS = {"restaurant", "cafe_coffee", "beverage", "bakery_dessert", "fast_food", "bar", "other_food", "non_fnb", "unknown"}
HISTORY_SCHEMA = pa.schema([
    *[(f, pa.string()) for f in ("decision_id", "parent_decision_id", "decision_type", "review_id", "review_grain", *KEY,
        "human_label", "reviewed_fnb_status", "reviewed_category", "reviewer", "reviewer_note", "evidence_hash",
        "er_release_id", "category_release_id", "google_silver_release_id", "overture_source_release",
        "er_rule_version", "category_rule_version", "category_mapping_version", "review_schema_version",
        "decision_rule_version", "input_csv_path", "input_csv_sha256", "imported_at_utc")],
    ("revision", pa.int64()),
])
IDENTITY_SCHEMA = pa.schema([
    *[(f, pa.string()) for f in (*KEY, "overture_name", "google_name", "overture_address", "google_address",
        "er_classification", "er_status", "proposal_status", "effective_status", "human_label", "reviewer",
        "reviewer_note", "decision_id", "review_id", "decision_evidence_hash", "evidence_applicability",
        "reason", "conflict_evidence_json", "observation_ref", "er_release_id", "category_release_id", "rule_version")],
    ("revision", pa.int64()), ("conflict_codes", pa.list_(pa.string())),
    ("shared_google_identity", pa.bool_()), ("multiple_google_candidates", pa.bool_()),
])
CATEGORY_SCHEMA = pa.schema([
    *[(f, pa.string()) for f in ("review_id", "review_grain", *KEY, "reviewed_fnb_status", "reviewed_category",
        "reviewer", "reviewer_note", "decision_id", "evidence_hash", "decision_status", "applicability_status",
        "category_evidence_json", "er_release_id", "category_release_id", "rule_version")],
    ("revision", pa.int64()),
])
INVENTORY_SCHEMA = pa.schema([(f, pa.string()) for f in ("overture_id", "overture_name", "overture_address", "status")])
PAYLOAD = ("human_label", "reviewed_fnb_status", "reviewed_category", "reviewer", "reviewer_note", "evidence_hash",
    "er_release_id", "category_release_id", "google_silver_release_id", "overture_source_release",
    "er_rule_version", "category_rule_version", "category_mapping_version", "review_schema_version")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def key(row):
    return tuple(row.get(k) or "" for k in KEY)


def read_csv(path):
    with Path(path).open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        fields = reader.fieldnames or []
        require(len(set(fields)) == len(fields), "Duplicate CSV column")
        rows = list(reader)
    require(all(None not in r and all(v is not None for v in r.values()) for r in rows), "Malformed CSV row")
    return rows, fields


def validate_rows(path, expected, kind):
    rows, fields = read_csv(path)
    labels = review.IDENTITY_LABELS if kind == "identity" else review.CATEGORY_LABELS
    contract = review.IDENTITY_FIELDS if kind == "identity" else review.CATEGORY_FIELDS
    require(set(contract) <= set(fields), "Missing review CSV columns: " + str(set(contract) - set(fields)))
    known_extra = {"verification_rank", "verification_priority", "verification_status", "verification_risk_codes",
        "verification_note", "related_google_group_json", "related_source_candidates_json", "ai_external_source_verification"}
    require(all(f in contract or f.startswith("ai_") or f in known_extra for f in fields), "Unknown CSV columns; only exported evidence, human annotation and ignored AI/audit fields are allowed")
    trusted = {r["review_id"]: r for r in expected}
    seen, seen_keys, decisions, warnings = set(), set(), [], []
    for row in rows:
        rid = row["review_id"]
        require(rid not in seen and key(row) not in seen_keys, "Duplicate review ID or composite key: " + rid)
        seen.add(rid); seen_keys.add(key(row))
        require(rid in trusted, "Unknown/stale review ID: " + rid)
        original = trusted[rid]
        for field in contract:
            if field not in labels:
                require(row[field] == original[field], f"Immutable evidence mismatch: {rid}, column={field}")
        # AI suggestions, confidence, rationale, status and priorities are never labels.
        annotation = {f: row[f].strip() for f in labels}
        if not any(annotation.values()):
            continue
        reviewer, note = annotation["reviewer"], annotation["reviewer_note"]
        label = annotation["human_label" if kind == "identity" else "reviewed_fnb_status"].upper()
        allowed = {"MATCH", "NON_MATCH", "UNSURE"} if kind == "identity" else {"FNB", "NON_FNB", "UNSURE"}
        require(not label or label in allowed, "Invalid human label: " + rid)
        category = annotation.get("reviewed_category", "").lower()
        require(not category or category in GROUPS, "Invalid reviewed_category: " + rid)
        if not label or not reviewer or not note:
            warnings.append(dict(review_id=rid, code="INCOMPLETE_HUMAN_ANNOTATION", reason="Label, reviewer and reviewer_note are all required; no decision imported and prior decision retained."))
            continue
        if kind == "category":
            require(not (label == "FNB" and category == "non_fnb") and not (label == "NON_FNB" and category not in {"", "non_fnb", "unknown"})
                    and not (label == "UNSURE" and category not in {"", "unknown"}), "Contradictory category annotation: " + rid)
        candidate = {f: original[f] for f in ("review_id", "review_grain", *KEY, *PAYLOAD) if f in original}
        candidate.update(decision_type=kind, human_label=label if kind == "identity" else "",
            reviewed_fnb_status=label if kind == "category" else "", reviewed_category=category,
            reviewer=reviewer, reviewer_note=note, input_csv_path=str(Path(path).resolve()), input_csv_sha256=silver.sha(path))
        candidate.update({f: original[f] or None for f in KEY})
        decisions.append(candidate)
    return decisions, dict(rows=len(rows), complete_human_rows=len(decisions), blank_or_incomplete_rows=len(rows)-len(decisions),
        ignored_ai_columns=sorted(f for f in fields if f.startswith("ai_")), warnings=warnings)


def latest_history(history):
    latest, ids = {}, set()
    for event in history:
        k = (event["decision_type"], event["review_id"])
        previous = latest.get(k)
        require(event["decision_id"] not in ids and event["revision"] == (previous["revision"] + 1 if previous else 1)
            and event["parent_decision_id"] == (previous["decision_id"] if previous else None), "Broken decision history chain")
        require(event["decision_rule_version"] == VERSION, "Unsupported decision history version")
        ids.add(event["decision_id"]); latest[k] = event
    return latest


def append_decisions(history, candidates, imported_at):
    latest = latest_history(history)
    combined, added = list(history), []
    for candidate in sorted(candidates, key=lambda r: (r["decision_type"], r["review_id"])):
        k = (candidate["decision_type"], candidate["review_id"])
        previous = latest.get(k)
        if previous and all(previous.get(f) == candidate.get(f) for f in PAYLOAD):
            continue
        event = {f: candidate.get(f) for f in HISTORY_SCHEMA.names}
        event.update(parent_decision_id=previous["decision_id"] if previous else None,
            revision=previous["revision"]+1 if previous else 1, decision_rule_version=VERSION, imported_at_utc=imported_at)
        event["decision_id"] = "human_" + _digest({f: event[f] for f in ("decision_type", "review_id", "parent_decision_id", "revision", *PAYLOAD)})
        combined.append(event); added.append(event); latest[k] = event
    latest_history(combined)
    return combined, added


def effective(history, data, expected):
    er_rows, category_rows, _, _, context = data
    latest = latest_history(history)
    identity_reviews = {key(r): r for r in expected[0]}
    labels, chosen = {}, {}
    for er in er_rows:
        k = key(er); r = identity_reviews.get(k)
        rid = r["review_id"] if r else "identity_" + _digest(dict(grain="LINK", key=list(k)))
        decision = latest.get(("identity", rid))
        chosen[k] = decision
        labels[k] = decision["human_label"] if decision and r and decision["evidence_hash"] == r["evidence_hash"] else None
    by_google, by_overture = defaultdict(list), defaultdict(list)
    for er in er_rows:
        by_google[key(er)[1:]].append(key(er)); by_overture[er["overture_id"]].append(key(er))
    identities = []
    for er in sorted(er_rows, key=key):
        k = key(er); decision = chosen[k]; label = labels[k]; codes = []
        status = "REJECTED" if label == "NON_MATCH" else "PENDING"
        if decision and label is None:
            codes.append("STALE_HUMAN_EVIDENCE")
        if label == "MATCH":
            for group, prefix in ((by_google[k[1:]], "SHARED_GOOGLE_ID"), (by_overture[k[0]], "MULTIPLE_GOOGLE_CANDIDATES")):
                if len(group) > 1:
                    if sum(labels[g] == "MATCH" for g in group) > 1:
                        codes.append(prefix + "_MULTIPLE_HUMAN_MATCHES")
                    elif any(labels[g] != "NON_MATCH" for g in group if g != k):
                        codes.append(prefix + "_UNRESOLVED")
            status = "PENDING" if codes else "ACCEPTED"
        neighbors = sorted(set(by_google[k[1:]] + by_overture[k[0]]))
        evidence = [dict(key=list(g), current_human_label=labels[g], decision_id=chosen[g]["decision_id"] if chosen[g] else None) for g in neighbors]
        reason = (f"Human {label}, reviewer={decision['reviewer']!r}; effective={status}. " if label else "No current, complete human decision; ER proposal remains provisional. ")
        if codes:
            reason += f"Unresolved checks={codes}; compare group decisions {silver.dump(evidence)}."
        identities.append(dict(**dict(zip(KEY, k)), **{f: er[f] for f in ("overture_name", "google_name", "overture_address", "google_address")},
            er_classification=er["classification"], er_status=er["er_status"], proposal_status="PROVISIONAL", effective_status=status,
            human_label=label, reviewer=decision["reviewer"] if decision else None, reviewer_note=decision["reviewer_note"] if decision else None,
            decision_id=decision["decision_id"] if decision else None, revision=decision["revision"] if decision else None,
            review_id=identity_reviews.get(k, {}).get("review_id") or (decision["review_id"] if decision else None), decision_evidence_hash=decision["evidence_hash"] if decision else None,
            evidence_applicability="CURRENT" if label else "STALE" if decision else "NO_DECISION", reason=reason,
            conflict_codes=sorted(codes), conflict_evidence_json=silver.dump(evidence), observation_ref=er["observation_ref"],
            shared_google_identity=len(by_google[k[1:]]) > 1, multiple_google_candidates=len(by_overture[k[0]]) > 1,
            er_release_id=context["er_release_id"], category_release_id=context["category_release_id"], rule_version=VERSION))
    status_by_key = {key(r): r["effective_status"] for r in identities}
    categories = []
    for r in expected[1]:
        decision = latest.get(("category", r["review_id"]))
        current = bool(decision and decision["evidence_hash"] == r["evidence_hash"])
        state = decision["reviewed_fnb_status"] if current else None
        applicability = ("PENDING" if not current or state == "UNSURE" else "SOURCE_ONLY_DECISION_NO_CANONICAL_MAPPING" if r["review_grain"] == "OVERTURE_SOURCE_ONLY"
            else "IDENTITY_ACCEPTED_CATEGORY_RECORDED" if status_by_key.get(key(r)) == "ACCEPTED" else "IDENTITY_REJECTED_DO_NOT_TRANSFER" if status_by_key.get(key(r)) == "REJECTED" else "WAITING_ACCEPTED_IDENTITY")
        categories.append(dict(review_id=r["review_id"], review_grain=r["review_grain"], **dict(zip(KEY, (v or None for v in key(r)))),
            reviewed_fnb_status=state, reviewed_category=decision["reviewed_category"] if current else None,
            reviewer=decision["reviewer"] if decision else None, reviewer_note=decision["reviewer_note"] if decision else None,
            decision_id=decision["decision_id"] if decision else None, revision=decision["revision"] if decision else None,
            evidence_hash=r["evidence_hash"], decision_status="STALE" if decision and not current else "PENDING" if not state or state == "UNSURE" else "HUMAN_RECORDED",
            applicability_status=applicability, category_evidence_json=r["source_evidence_json"],
            er_release_id=context["er_release_id"], category_release_id=context["category_release_id"], rule_version=VERSION))
    inventory = [dict(overture_id=r["overture_id"], overture_name=r["overture_name"], overture_address=r["overture_address"], status="NO_GOOGLE_CANDIDATE")
        for r in category_rows if r["baseline_fnb_candidate"] and not r["google_id_kind"]]
    require(len(identities) == len(er_rows) and len({key(r) for r in identities}) == len(er_rows), "Effective link coverage loss")
    accepted = [r for r in identities if r["effective_status"] == "ACCEPTED"]
    require(all(r["human_label"] == "MATCH" and not r["conflict_codes"] and r["decision_id"] for r in accepted), "Unsafe accepted identity")
    require(len({r["overture_id"] for r in accepted}) == len(accepted) and len({key(r)[1:] for r in accepted}) == len(accepted), "Inconsistent accepted mapping")
    return {"decision_history.parquet": pa.Table.from_pylist(history, schema=HISTORY_SCHEMA),
        "effective_identity_mapping.parquet": pa.Table.from_pylist(identities, schema=IDENTITY_SCHEMA),
        "effective_category_decisions.parquet": pa.Table.from_pylist(categories, schema=CATEGORY_SCHEMA),
        "unlinked_overture_inventory.parquet": pa.Table.from_pylist(sorted(inventory, key=lambda r: r["overture_id"]), schema=INVENTORY_SCHEMA)}


def plan(data, history, identity_csv, category_csv, imported_at):
    identity, category, _ = review.build(*data)
    id_rows, id_info = validate_rows(identity_csv, identity, "identity")
    cat_rows, cat_info = validate_rows(category_csv, category, "category")
    history, added = append_decisions(history, id_rows + cat_rows, imported_at)
    tables = effective(history, data, (identity, category))
    mapping = tables["effective_identity_mapping.parquet"].to_pylist()
    summary = dict(rule_version=VERSION, identity_input=id_info, category_input=cat_info, new_decisions=len(added),
        new_identity_decisions=sum(d["decision_type"] == "identity" for d in added),
        new_category_decisions=sum(d["decision_type"] == "category" for d in added), history_rows=len(history),
        effective_identity_counts={state: sum(r["effective_status"] == state for r in mapping) for state in ("ACCEPTED", "REJECTED", "PENDING")},
        effective_category_counts=dict(sorted(Counter(r["decision_status"] for r in tables["effective_category_decisions.parquet"].to_pylist()).items())),
        mapping_conflict_links=sum(bool(r["conflict_codes"]) for r in mapping),
        shared_google_identity_links=sum(r["shared_google_identity"] for r in mapping),
        multiple_google_candidate_links=sum(r["multiple_google_candidates"] for r in mapping),
        no_google_candidate_ids=tables["unlinked_overture_inventory.parquet"].num_rows,
        source_context={f: data[4][f] for f in ("er_release_id", "category_release_id", "google_silver_release_id")},
        limitations=["Reviewer and note are required attestations; software cannot prove who physically entered them.",
            "Blank/incomplete annotations do not revoke an earlier decision; explicit UNSURE creates a pending revision.",
            "Accepted links do not merge Overture IDs or allocate canonical store IDs.",
            "Category decisions do not change identity status or source taxonomy."])
    return tables, summary


def read_history(output):
    active = silver.read_active(output)
    if active is None:
        return None, []
    manifest = silver.verify_release(active)
    require(manifest["signature"]["rule_version"] == VERSION, "Different dataset at decision output root")
    history = pq.read_table(active / "decision_history.parquet").to_pylist()
    latest_history(history)
    return active, history


def run(er_root, category_root, identity_csv, category_csv, output_root, *, apply=False, fault=lambda stage: None):
    data, hashes, roots = review.load(er_root, category_root)
    output = Path(output_root).resolve()
    require(all(not output.is_relative_to(p.resolve()) and not p.resolve().is_relative_to(output) for p in roots), "Separate source/output roots required")
    for path in (identity_csv, category_csv):
        path = Path(path).resolve()
        require(not path.is_relative_to(output), "Decision output cannot contain input CSV")
        hashes[str(path)] = silver.sha(path)
    code_paths = [Path(__file__), Path(review.__file__), Path(silver.__file__), (Path(__file__).resolve().parents[1] / "overture_incremental.py"), Path(publication.__file__)]
    code_hashes = {str(p.resolve()): silver.sha(p) for p in code_paths}
    def prepare():
        pointer = output / "current.json"
        before = silver.sha(pointer) if pointer.exists() else None
        active, history = read_history(output)
        tables, summary = plan(data, history, identity_csv, category_csv, datetime.now(timezone.utc).isoformat())
        for p, h in {**hashes, **code_hashes}.items():
            require(silver.sha(p) == h, "Input/code changed during import: " + p)
        require((silver.sha(pointer) if pointer.exists() else None) == before, "Decision state changed during planning; retry")
        return active, tables, summary
    if not apply:
        active, _, summary = prepare()
        return dict(summary, mode="DRY_RUN", writes=0, active_release=str(active) if active else None)
    output.mkdir(parents=True, exist_ok=True)
    with publication_lock(output):
        active, tables, summary = prepare()
        if active:
            existing = silver.verify_release(active)
            if summary["new_decisions"] == 0 and existing["signature"]["source_context"] == summary["source_context"] and all(
                    pq.read_table(active / name).equals(table) for name, table in tables.items()) and existing["signature"]["code_sha256"] == code_hashes:
                return dict(summary, mode="APPLY", idempotent=True, active_release=str(active))
        signature = dict(rule_version=VERSION, source_context=summary["source_context"], inputs_sha256=hashes,
            code_sha256=code_hashes, parent_release_id=active.name if active else None,
            logical_sha256={name: _digest(table.to_pylist()) for name, table in tables.items()}, pyarrow_version=pa.__version__)
        rid = _digest(signature)[:24]; dest = output / "releases" / rid
        stage = output / (".stage-" + uuid.uuid4().hex)
        stage.mkdir()
        try:
            artifacts = {}
            for name, table in tables.items():
                pq.write_table(table, stage / name, compression="zstd")
                artifacts[name] = dict(rows=table.num_rows, sha256=silver.sha(stage / name), logical_sha256=_digest(table.to_pylist()), schema=str(table.schema))
            (stage / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
            artifacts["summary.json"] = dict(sha256=silver.sha(stage / "summary.json"))
            (stage / "manifest.json").write_text(silver.dump(dict(release_id=rid, signature=signature, artifacts=artifacts, publication_kind="HUMAN_DECISIONS_NOT_GOLD")), encoding="utf-8")
            silver.verify_release(stage); fault("after_stage_validation")
            for p, h in {**hashes, **code_hashes}.items():
                require(silver.sha(p) == h, "Input/code changed before activation: " + p)
            dest.parent.mkdir(exist_ok=True)
            if dest.exists():
                require(silver.verify_release(dest)["signature"] == signature, "Release signature conflict")
            else:
                os.replace(stage, dest)
            fault("after_release_rename")
            pointer = output / (".current-" + uuid.uuid4().hex)
            pointer.write_text(silver.dump(dict(release_id=rid, manifest_sha256=silver.sha(dest / "manifest.json"))), encoding="utf-8")
            fault("before_pointer_replace")
            for p, h in {**hashes, **code_hashes}.items():
                require(silver.sha(p) == h, "Input/code changed before pointer replacement: " + p)
            os.replace(pointer, output / "current.json")
            fault("after_pointer_replace")
            return dict(summary, mode="APPLY", idempotent=False, active_release=str(dest))
        finally:
            if stage.exists():
                require(stage.resolve().parent == output and stage.name.startswith(".stage-"), "Unsafe stage cleanup")
                shutil.rmtree(stage)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("er-root", "category-root", "identity-csv", "category-csv", "output-root"):
        parser.add_argument("--" + name, type=Path, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true"); mode.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    try:
        print(silver.dump(run(args.er_root, args.category_root, args.identity_csv, args.category_csv, args.output_root, apply=args.apply)))
    except (ValueError, OSError) as error:
        parser.exit(2, f"Human import rejected: {error}\n")


if __name__ == "__main__":
    main()
