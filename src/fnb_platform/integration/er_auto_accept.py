"""Hybrid ER Auto-Accept V1, shadow evaluation only. Human history is read-only."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import json
import math
import os
from pathlib import Path
import shutil
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

from .. import er_category_review_export as review, human_decisions as human, google_silver as silver, publication
from ..overture_incremental import _digest

VERSION = "er-hybrid-shadow-v1"
DEFAULT_POLICY = Path(__file__).resolve().parents[3] / "config/er_auto_accept_policy_v1.json"
KEY = review.KEY
STATUSES = ("AUTO_ACCEPTED", "HUMAN_ACCEPTED", "REJECTED", "PENDING", "CONFLICT")
SCHEMA = pa.schema([
    *[(f, pa.string()) for f in (*KEY, "overture_name", "google_name", "overture_address", "google_address",
        "er_classification", "er_status", "effective_status", "decision_source", "policy_version", "reason",
        "evidence_hash", "evidence_json", "source_provenance_json", "human_label", "human_decision_id",
        "human_evidence_hash", "human_evidence_applicability", "human_reviewer", "human_reviewer_note", "ai_suggested_label", "ai_confidence",
        "ai_status", "ai_policy_relation", "category_relation", "metadata_status", "er_release_id",
        "category_release_id", "google_silver_release_id", "rule_version")],
    *[(f, pa.list_(pa.string())) for f in ("reason_codes", "policy_conflicts", "support_gaps")],
    *[(f, pa.bool_()) for f in ("policy_candidate", "positive_evidence_pass", "shared_google_identity",
        "multiple_google_candidates", "shadow_only")],
    ("distance_m", pa.float64()), ("name_similarity", pa.float64()),
])


def require(condition, message):
    if not condition:
        raise ValueError(message)


def key(row):
    return human.key(row)


def load_policy(path):
    p = json.loads(Path(path).read_text(encoding="utf-8"))
    require(p.get("policy_version") == "er-auto-accept-v1" and p.get("execution_mode") == "SHADOW_ONLY", "Only V1 SHADOW_ONLY policy is supported")
    require(p.get("candidate_entry") == ["ER_V3_MATCH_CANDIDATE", "AUDITED_AI_MATCH"] and p.get("required_v3_signals") == ["name", "address", "location"]
        and p.get("required_signal_value") == "SUPPORT", "Unsupported policy evidence contract")
    require(all(p.get(f) is True for f in ("require_strong_address_agreement", "require_specific_address_agreement", "block_shared_google_identity",
        "block_any_metadata_conflict", "ai_unsure_stays_pending", "ai_non_match_never_rejects"))
        and p.get("canonical_identity_merge") is False and p.get("resolve_multiple_candidates_with") == "CURRENT_HUMAN_NON_MATCH_ONLY", "Unsafe/unsupported V1 policy settings")
    return p


def audited_ai(path, audit_path, expected, context):
    """Recheck audited suggestions against live pinned evidence; never import labels."""
    path, audit_path = Path(path).resolve(), Path(audit_path).resolve()
    report = json.loads(audit_path.read_text(encoding="utf-8"))
    require(report.get("status") == "PROVISIONAL_AI_ONLY_NO_DECISIONS_IMPORTED" and all(report.get("integrity_checks", {}).values()) and bool(report.get("integrity_checks")), "AI audit integrity contract missing")
    require(report["releases"] == {f: context[f] for f in ("er_release_id", "category_release_id", "google_silver_release_id")}, "Stale AI audit release")
    pinned = [digest for p, digest in report["input_hashes"].items() if Path(p).resolve() == path]
    require(len(pinned) == 1 and pinned[0] == silver.sha(path), "AI file differs from audited bytes")
    rows, fields = human.read_csv(path)
    require(set(review.IDENTITY_FIELDS) | {"ai_suggested_label", "ai_confidence", "ai_status", "ai_rationale"} <= set(fields), "Missing audited AI columns")
    trusted = {r["review_id"]: r for r in expected}
    result, seen = {}, set()
    for row in rows:
        rid = row["review_id"]
        require(rid in trusted and rid not in seen and key(row) not in result, "Unknown/duplicate AI identity")
        seen.add(rid)
        require(all(row[f] == trusted[rid][f] for f in review.IDENTITY_FIELDS if f not in review.IDENTITY_LABELS), "AI immutable evidence mismatch: " + rid)
        require(all(not row[f].strip() for f in review.IDENTITY_LABELS), "AI triage must not contain human annotations; use the human importer separately")
        require(row["ai_suggested_label"] in {"MATCH", "NON_MATCH", "UNSURE"} and row["ai_status"] == "AI_PRE_REVIEW_NOT_APPROVED", "Invalid/non-provisional AI suggestion")
        result[key(row)] = {f: row.get(f, "") for f in ("review_id", "ai_suggested_label", "ai_confidence", "ai_status", "ai_rationale", "ai_external_source")}
    require(seen == set(trusted) and len(rows) == report["rows"], "AI review coverage mismatch")
    require(dict(Counter(r["ai_suggested_label"] for r in rows)) == report["ai_suggested_labels"], "AI audit label counts mismatch")
    return result


from .matching_policy import coordinate_valid, gates


def build(data, google_rows, history, ai, policy, provenance):
    expected = review.build(*data)[:2]
    base = human.effective(history, data, expected)["effective_identity_mapping.parquet"].to_pylist()
    human_rows = {key(r): r for r in base}
    current_human = {key(r): r["human_label"] for r in base}
    cats = {key(r): r for r in data[1] if r["google_id_kind"] and r["overture_id"]}
    google = review.index(google_rows, lambda r: (r["google_id_kind"], r["google_identity_value"]), "Google")
    by_google, by_source = defaultdict(list), defaultdict(list)
    for er in data[0]:
        by_google[key(er)[1:]].append(key(er)); by_source[er["overture_id"]].append(key(er))
    rows = []
    for er in sorted(data[0], key=key):
        k = key(er); cat = cats[k]; gp = google[k[1:]]; hm = human_rows[k]; machine = ai.get(k, {})
        label = machine.get("ai_suggested_label"); human_label = hm["human_label"]
        conflicts, gaps, observations = gates(er, cat, gp, (by_google[k[1:]], by_source[k[0]]), current_human, policy)
        positive = not conflicts and not gaps
        candidate = er["classification"] == "MATCH_CANDIDATE" or label == "MATCH"
        if not label:
            ai_relation = "NO_AI_SUGGESTION"
        elif label == "MATCH":
            ai_relation = "AI_MATCH_SUPPORTED" if positive else "AI_MATCH_BLOCKED_BY_CONFLICT" if conflicts else "AI_MATCH_INSUFFICIENT_SUPPORT"
        elif label == "NON_MATCH":
            ai_relation = "AI_NON_MATCH_VS_POSITIVE_EVIDENCE" if positive else "AI_NON_MATCH_NOT_ACCEPTED"
        else:
            ai_relation = "AI_UNSURE_HELD"
        codes = [*conflicts, *gaps]
        if human_label == "MATCH":
            status = "HUMAN_ACCEPTED" if hm["effective_status"] == "ACCEPTED" else "CONFLICT"
            source = "HUMAN"; codes += ["HUMAN_MATCH_CURRENT", *hm["conflict_codes"]]
        elif human_label == "NON_MATCH":
            status, source = "REJECTED", "HUMAN"; codes += ["HUMAN_NON_MATCH_OVERRIDE"]
        elif human_label == "UNSURE":
            status, source = "PENDING", "HUMAN"; codes += ["HUMAN_UNSURE_OVERRIDE"]
        elif hm["evidence_applicability"] == "STALE":
            status, source = "PENDING", "HUMAN"; codes += ["STALE_HUMAN_EVIDENCE"]
        elif label == "UNSURE":
            status, source = "PENDING", "MACHINE_SUGGESTION"; codes += ["AI_UNSURE_PENDING"]
        elif label == "NON_MATCH":
            status, source = ("CONFLICT" if positive or conflicts else "PENDING"), "MACHINE_SUGGESTION"
            codes += ["AI_NON_MATCH_IS_NOT_HUMAN_REJECTION"]
            if positive:
                codes += ["AI_NON_MATCH_VS_POSITIVE_EVIDENCE"]
        elif candidate and positive:
            status, source = "AUTO_ACCEPTED", "AUTO_POLICY"; codes += ["POLICY_POSITIVE_EVIDENCE_AND_NO_CONFLICT"]
        else:
            status, source = ("CONFLICT" if candidate and conflicts else "PENDING"), "AUTO_POLICY" if candidate else "ER_PROPOSAL"
            if not candidate:
                codes += ["NO_AUTO_CANDIDATE_ENTRY"]
        if er["classification"] == "MATCH_CANDIDATE":
            codes.append("ER_V3_MATCH_CANDIDATE_ENTRY")
        if label == "MATCH":
            codes.append("AUDITED_AI_MATCH_ENTRY_NOT_A_HUMAN_LABEL")
        neighbors = sorted(set(by_google[k[1:]] + by_source[k[0]]))
        group_evidence = [dict(key=list(g), human_label=current_human[g], decision_id=human_rows[g]["decision_id"]) for g in neighbors]
        evidence = dict(key=dict(zip(KEY, k)), names={"overture": er["overture_name"], "google": er["google_name"]},
            addresses={"overture": er["overture_address"], "google": er["google_address"]},
            categories={"overture_raw":json.loads(cat["overture_categories_raw"] or "null"),
                "google_raw":json.loads(cat["google_categories_raw"] or "null"),
                "overture_normalized":cat["overture_categories"], "google_normalized":cat["google_categories"]},
            observations=observations, category_relation=cat["category_relation"], groups=group_evidence,
            original_er_evidence_sha256=_digest(json.loads(er["evidence_json"])), category_evidence_sha256=_digest(json.loads(cat["evidence_json"])),
            google_metadata_sha256=_digest(gp), human_decision_id=hm["decision_id"], human_evidence_hash=hm["decision_evidence_hash"],
            ai_suggestion=machine, policy=policy, source_context=data[4], source_provenance=provenance,
            effective_status=status, decision_source=source, reason_codes=sorted(set(codes)))
        reason = f"{er['overture_name']!r} ↔ {er['google_name']!r}: shadow={status}, source={source}; name/address/location={observations['signals']}; distance_m={er['distance_m']}; category={cat['category_relation']}; conflicts={conflicts}; missing_support={gaps}."
        if source == "HUMAN":
            reason += " Current human decision has priority; " + hm["reason"]
        if label:
            reason += f" AI={label} remains a machine suggestion ({ai_relation})."
        rows.append(dict(**dict(zip(KEY, k)), **{f: er[f] for f in ("overture_name", "google_name", "overture_address", "google_address", "distance_m", "name_similarity")},
            er_classification=er["classification"], er_status=er["er_status"], effective_status=status, decision_source=source,
            policy_version=policy["policy_version"], reason=reason, reason_codes=sorted(set(codes)), policy_conflicts=conflicts,
            support_gaps=gaps, evidence_hash=_digest(evidence), evidence_json=silver.dump(evidence), source_provenance_json=silver.dump(provenance),
            human_label=human_label, human_decision_id=hm["decision_id"], human_evidence_hash=hm["decision_evidence_hash"],
            human_evidence_applicability=hm["evidence_applicability"],
            human_reviewer=hm["reviewer"], human_reviewer_note=hm["reviewer_note"], ai_suggested_label=label,
            ai_confidence=machine.get("ai_confidence"), ai_status=machine.get("ai_status"), ai_policy_relation=ai_relation,
            category_relation=cat["category_relation"], metadata_status=gp["metadata_status"], policy_candidate=candidate,
            positive_evidence_pass=positive, shared_google_identity=len(by_google[k[1:]])>1,
            multiple_google_candidates=len(by_source[k[0]])>1, shadow_only=True,
            **{f: data[4][f] for f in ("er_release_id", "category_release_id", "google_silver_release_id")}, rule_version=VERSION))
    accepted = [r for r in rows if r["effective_status"] in {"AUTO_ACCEPTED", "HUMAN_ACCEPTED"}]
    auto = [r for r in rows if r["effective_status"] == "AUTO_ACCEPTED"]
    require(len(rows) == len(data[0]) and len({key(r) for r in rows}) == len(rows), "Shadow coverage loss")
    require(len({r["overture_id"] for r in accepted}) == len(accepted) and len({key(r)[1:] for r in accepted}) == len(accepted), "Unauthorized many-to-one mapping/merge")
    require(all(not r["policy_conflicts"] and not r["support_gaps"] and r["decision_source"] == "AUTO_POLICY" and not r["human_label"] for r in auto), "Unsafe auto acceptance")
    require(all(r["effective_status"] != "REJECTED" or r["human_label"] == "NON_MATCH" for r in rows), "Machine suggestion used as human rejection")
    require(all(r["effective_status"] == "PENDING" for r in rows if r["human_label"] == "UNSURE" or (not r["human_label"] and r["ai_suggested_label"] == "UNSURE")), "UNSURE override violated")
    er_candidates = [r for r in rows if r["er_classification"] == "MATCH_CANDIDATE"]
    held = [r for r in er_candidates if r["effective_status"] != "AUTO_ACCEPTED"]
    original_review = [r for r in rows if r["er_classification"] == "REVIEW"]
    counts = lambda rs: {s: sum(r["effective_status"] == s for r in rs) for s in STATUSES}
    summary = dict(mode="SHADOW_ONLY_NOT_ACTIVATED", policy_version=policy["policy_version"], total_links=len(rows),
        effective_status_counts=counts(rows), er_v3_match_candidates=dict(rows=len(er_candidates), statuses=counts(er_candidates), held=len(held),
            primary_hold_reason_counts=dict(sorted(Counter(next((c for c in r["reason_codes"] if c.endswith("OVERRIDE") or c == "AI_UNSURE_PENDING"),
                (r["policy_conflicts"] or r["support_gaps"] or ["HUMAN_DECISION_PRIORITY"])[0]) for r in held).items())),
            all_hold_reason_counts=dict(sorted(Counter(c for r in held for c in [*r["policy_conflicts"], *r["support_gaps"]]).items()))),
        original_90_review=dict(rows=len(original_review), statuses=counts(original_review), machine_suggestions=dict(Counter(r["ai_suggested_label"] for r in original_review))),
        ai_suggested_labels=dict(Counter(r["ai_suggested_label"] for r in rows if r["ai_suggested_label"])),
        ai_effective_statuses={label:counts([r for r in rows if r["ai_suggested_label"] == label]) for label in ("MATCH", "NON_MATCH", "UNSURE")},
        ai_policy_relations=dict(sorted(Counter(r["ai_policy_relation"] for r in rows if r["ai_suggested_label"]).items())),
        machine_policy_disagreements=dict(ai_match_with_hard_blockers=sum(r["ai_policy_relation"] == "AI_MATCH_BLOCKED_BY_CONFLICT" for r in rows),
            ai_non_match_with_positive_policy_evidence=sum(r["ai_policy_relation"] == "AI_NON_MATCH_VS_POSITIVE_EVIDENCE" for r in rows),
            note="A conservative policy hold is not proof that an AI identity suggestion is wrong."),
        auto_safety_checks=dict(shared_google_id_leaks=sum(r["shared_google_identity"] for r in auto),
            category_conflict_leaks=sum(r["category_relation"] == "CATEGORY_CONFLICT" for r in auto),
            unresolved_multiple_candidate_leaks=sum("MULTIPLE_GOOGLE_CANDIDATES_UNRESOLVED" in r["policy_conflicts"] for r in auto),
            metadata_conflict_leaks=sum("METADATA_CONFLICT" in r["policy_conflicts"] for r in auto), unauthorized_merges=0),
        no_google_candidate_ids=sum(r["baseline_fnb_candidate"] and not r["google_id_kind"] for r in data[1]),
        human_history_rows=len(history), limitations=["Shadow only; official human mapping/history unchanged.",
            "No ground-truth precision/recall estimate; AI confidence is uncalibrated.",
            "Existing V3 support signals/coordinate band are reused; no new similarity/distance cutoff.",
            "Uncertain, reference-only and missing-address evidence is held, not treated as a proven mismatch.",
            "Category agreement does not confirm identity; accepted links do not create store IDs."])
    return pa.Table.from_pylist(rows, schema=SCHEMA), summary


def publish(er_root, category_root, human_root, ai_csv, ai_audit, output_root, policy_path=DEFAULT_POLICY, *, fault=lambda stage: None):
    data, hashes, roots = review.load(er_root, category_root)
    output, human_root = Path(output_root).resolve(), Path(human_root).resolve()
    for root in [*roots, human_root]:
        require(not output.is_relative_to(root.resolve()) and not root.resolve().is_relative_to(output), "Separate shadow/source/history roots required")
    expected = review.build(*data)[0]
    policy = load_policy(policy_path)
    machine = audited_ai(ai_csv, ai_audit, expected, data[4])
    human_pointer = human_root / "current.json"
    pointer_hash = silver.sha(human_pointer) if human_pointer.exists() else None
    active, history = human.read_history(human_root)
    require((silver.sha(human_pointer) if human_pointer.exists() else None) == pointer_hash, "Human state changed while reading history; replay")
    if active:
        for p in (human_pointer, active / "manifest.json", active / "decision_history.parquet"):
            hashes[str(p.resolve())] = silver.sha(p)
    gp = Path(data[4]["refs"]["google_places"]["path"])
    google = pq.read_table(gp, columns=["google_id_kind", "google_identity_value", "metadata_status", "latitude", "longitude", "has_metadata_conflict", "conflict_fields", "conflicting_values_json"]).to_pylist()
    for p in (ai_csv, ai_audit, policy_path):
        p = Path(p).resolve(); require(not p.is_relative_to(output), "Shadow output contains input")
        hashes[str(p)] = silver.sha(p)
    provenance = dict(input_hashes=hashes, human_release=str(active) if active else None, human_history_unchanged=True)
    table, summary = build(data, google, history, machine, policy, provenance)
    code = [Path(__file__), Path(human.__file__), Path(review.__file__), Path(silver.__file__), (Path(__file__).resolve().parents[1] / "overture_incremental.py"), Path(publication.__file__)]
    signature = dict(rule_version=VERSION, policy_version=policy["policy_version"], mode="SHADOW_ONLY", inputs_sha256=hashes,
        human_pointer_sha256=pointer_hash, code_sha256={str(p.resolve()): silver.sha(p) for p in code},
        logical_sha256=_digest(table.to_pylist()), pyarrow_version=pa.__version__)
    rid = _digest(signature)[:24]
    def check_inputs():
        for p, h in {**hashes, **signature["code_sha256"]}.items():
            require(silver.sha(p) == h, "Shadow input/code changed: " + p)
        require((silver.sha(human_pointer) if human_pointer.exists() else None) == pointer_hash, "Human state changed during shadow evaluation; replay")
    output.mkdir(parents=True, exist_ok=True)
    with human.publication_lock(output):
        current = silver.read_active(output)
        if current:
            require(silver.verify_release(current)["signature"]["rule_version"] == VERSION, "Different dataset at shadow root")
        dest = output / "releases" / rid
        stage = output / (".stage-" + uuid.uuid4().hex)
        try:
            if not dest.exists():
                stage.mkdir()
                pq.write_table(table, stage / "effective_identity_mapping.parquet", compression="zstd")
                summary["inputs"] = provenance
                (stage / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
                artifacts = {"effective_identity_mapping.parquet": dict(rows=table.num_rows, sha256=silver.sha(stage / "effective_identity_mapping.parquet"), logical_sha256=_digest(table.to_pylist()), schema=str(table.schema)),
                    "summary.json": dict(sha256=silver.sha(stage / "summary.json"))}
                (stage / "manifest.json").write_text(silver.dump(dict(release_id=rid, signature=signature, artifacts=artifacts, publication_kind="SHADOW_ONLY_NOT_OFFICIAL_MAPPING")), encoding="utf-8")
                silver.verify_release(stage); fault("after_stage_validation"); check_inputs()
                dest.parent.mkdir(exist_ok=True); os.replace(stage, dest)
            else:
                require(silver.verify_release(dest)["signature"] == signature, "Shadow replay signature mismatch")
                summary = json.loads((dest / "summary.json").read_text(encoding="utf-8"))
            fault("before_pointer_replace"); check_inputs()
            pointer = output / (".current-" + uuid.uuid4().hex)
            pointer.write_text(silver.dump(dict(release_id=rid, manifest_sha256=silver.sha(dest / "manifest.json"))), encoding="utf-8")
            os.replace(pointer, output / "current.json")
            return dest, summary
        finally:
            if stage.exists():
                require(stage.resolve().parent == output and stage.name.startswith(".stage-"), "Unsafe shadow stage cleanup")
                shutil.rmtree(stage)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("er-root", "category-root", "human-root", "ai-csv", "ai-audit", "output-root"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--shadow", action="store_true", required=True, help="Required; official activation is not implemented")
    args = parser.parse_args()
    dest, summary = publish(args.er_root, args.category_root, args.human_root, args.ai_csv, args.ai_audit, args.output_root, args.policy)
    print(silver.dump(dict(release=str(dest), mode=summary["mode"], statuses=summary["effective_status_counts"],
        match_candidates=summary["er_v3_match_candidates"], original_review=summary["original_90_review"], ai_relations=summary["ai_policy_relations"], safety=summary["auto_safety_checks"])))


if __name__ == "__main__":
    main()
