"""Export Excel review queues only. No decision import or mapping acceptance."""
from __future__ import annotations

import argparse
from collections import Counter
import csv
import json
import os
from pathlib import Path
import shutil
from urllib.parse import urlsplit
import uuid

import pyarrow.parquet as pq

from .. import google_silver as silver
from ..overture_incremental import _digest

VERSION = "er-category-human-review-export-v1"
KEY = ("overture_id", "google_id_kind", "google_identity_value")
IDENTITY_LABELS = ("human_label", "reviewer_note", "reviewer")
CATEGORY_LABELS = ("reviewed_fnb_status", "reviewed_category", "reviewer_note", "reviewer")
COMMON = ["review_id", "review_grain", *KEY, "overture_source_release", "google_silver_release_id",
    "er_release_id", "category_release_id", "er_rule_version", "category_rule_version",
    "category_mapping_version", "review_schema_version", "er_evidence_hash", "category_evidence_hash",
    "source_evidence_hash", "evidence_hash", "source_provenance_json", "input_refs_json"]
DISPLAY = ["priority", "priority_reason", "overture_name", "google_name", "overture_address", "google_address",
    "google_maps_url", "overture_categories_raw", "google_categories_raw", "overture_categories_normalized",
    "google_categories_normalized", "taxonomy_hierarchy", "distance_m", "name_similarity",
    "er_classification", "category_relation", "category_comparison_status", "shared_google_identity",
    "multiple_google_candidates", "baseline_fnb_candidate", "overture_inside_ward", "reason_codes",
    "er_reason", "category_reason", "er_evidence_json", "source_evidence_json", "er_observation_ref"]
IDENTITY_FIELDS = [*DISPLAY[:7], *IDENTITY_LABELS, *DISPLAY[7:], *COMMON]
CATEGORY_FIELDS = [*DISPLAY[:7], *CATEGORY_LABELS, *DISPLAY[7:], *COMMON]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def key(row):
    return tuple(row.get(k) for k in KEY)


def index(rows, key_fn, description):
    result = {}
    for row in rows:
        k = key_fn(row)
        require(k not in result, "Duplicate " + description + " key: " + str(k))
        result[k] = row
    return result


def observed_url(value):
    """Only retain an observed Google Maps URL. Never synthesize a Place ID."""
    if not isinstance(value, str):
        return ""
    parsed = urlsplit(value)
    return value if parsed.scheme == "https" and parsed.hostname in {"google.com", "www.google.com", "google.com.vn", "www.google.com.vn"} and parsed.path.startswith("/maps") else ""


def current_features(er):
    evidence = json.loads(er["evidence_json"]) if er else {}
    v3 = evidence.get("comparisons", {}).get("v3", {})
    name, address = v3.get("name_features", {}), v3.get("address_features_v3", {})
    branch = name.get("effective_branch_warning_codes", [])
    compact = dict(applied_rule=v3.get("applied_rule"), signals=v3.get("signals"),
        review_reason_codes=v3.get("review_reason_codes", []),
        name_features={k: name[k] for k in ("full_similarity", "token_dice", "core_coverage", "core_equal",
            "token_containment", "typo_support", "core_numeric_identifiers", "effective_branch_warning_codes") if k in name},
        address_features={k: address[k] for k in ("clear_conflict", "strong_agreement", "house_state", "street_state",
            "alley_state", "lot_state", "normalized_surface_equal") if k in address},
        compared_address={side: {k: address.get(side, {}).get(k) for k in ("house", "street", "alleys", "lots", "original_parse_status")}
                          for side in ("overture", "google")},
        coordinates={side: {k: evidence.get(side, {}).get(k) for k in ("latitude", "longitude")} for side in ("overture", "google")})
    return compact, bool(address.get("clear_conflict") or branch), v3.get("review_reason_codes", [])


def identity_priority(cat, er, current_conflict):
    if cat["category_relation"] == "CATEGORY_CONFLICT":
        return 1, "P1_TRUE_CATEGORY_CONFLICT", "F&B/non-F&B category evidence disagrees; verify business identity before accepting the link."
    if current_conflict or cat["multiple_google_candidates"]:
        return 2, "P2_CURRENT_IDENTITY_CONTRADICTION", "Current ER evidence has address/branch contradictions or multiple Google candidates."
    if er["classification"] == "NON_MATCH_CANDIDATE":
        return 2, "P2_NON_MATCH_PROPOSAL", "ER V3 proposes NON_MATCH; inspect its recorded evidence."
    if er["classification"] == "REVIEW":
        return 3, "P3_ER_REVIEW", "ER V3 requires review; no true category conflict was identified."
    if cat["shared_google_identity"]:
        return 4, "P4_SHARED_ID_WARNING", "Several Overture IDs selected one Google identity; sharing alone does not prove a wrong link or a merge."
    return 4, "P4_IDENTITY_WARNING", "Category audit requests identity recheck without a stronger current contradiction."


def strings(row):
    return {k: "" if v is None else silver.dump(v) if isinstance(v, (list, dict, bool)) else str(v) for k, v in row.items()}


def build(er_rows, category_rows, normalized_rows, google_rows, context):
    """Pure deterministic export. Evidence hashes pin source evidence, not Excel formatting."""
    er = index(er_rows, key, "ER")
    cats = index(category_rows, key, "category audit")
    require(all(r["rule_version"] == "ov-google-er-proposal-v3-experimental" for r in er_rows), "Unexpected ER evidence version")
    require(all(r["rule_version"] == "category-quality-v1" and r["mapping_version"] for r in category_rows), "Unexpected category evidence version")
    normal = index(normalized_rows, lambda r: (r["source"], r.get("source_id"), r.get("google_id_kind"), r.get("google_identity_value")), "normalization")
    google = index(google_rows, lambda r: (r["google_id_kind"], r["google_identity_value"]), "Google")
    require(all(all(isinstance(v, str) and v for v in k) for k in er), "Invalid ER composite key")
    require(set(er) == {k for k in cats if all(k)}, "ER/category relationship coverage mismatch")
    identities, categories = [], []
    for k, cat in sorted(cats.items(), key=lambda item: tuple(v or "" for v in item[0])):
        linked = all(k)
        require(linked or (k[0] and not k[1] and not k[2]) or (not k[0] and k[1] and k[2]), "Invalid category audit grain")
        e = er.get(k)
        if e:
            require(cat["er_classification"] == e["classification"] and cat["er_status"] == e["er_status"], "ER/category classification mismatch")
        identity_needed = bool(e and (e["classification"] == "REVIEW" or cat["category_relation"] == "CATEGORY_CONFLICT" or cat["comparison_status"] == "NEEDS_IDENTITY_RECHECK"))
        category_needed = cat["category_relation"] == "CATEGORY_CONFLICT" or "OVERTURE_FNB_NAME_HINT_TAXONOMY_EXCLUDED" in cat["reason_codes"]
        if not identity_needed and not category_needed:
            continue
        ov = normal.get(("overture", k[0], None, None)) if k[0] else None
        gn = normal.get(("google", None, k[1], k[2])) if k[1] else None
        require((not k[0] or ov) and (not k[1] or gn), "Missing source normalization record")
        g = google.get(k[1:]) if k[1] else None
        require(not k[1] or g, "Missing pinned Google record")
        compact, current_conflict, er_reasons = current_features(e)
        raw = json.loads(ov["raw_categories_json"]) if ov else {}
        source_evidence = {side: dict(raw=json.loads(r["raw_categories_json"]), normalized=r["normalized_categories"],
            comparison_evidence=json.loads(r["category_evidence_json"]), dq_flags=r["dq_flags"],
            provenance=json.loads(r["source_provenance_json"])) if r else None for side, r in (("overture", ov), ("google", gn))}
        base = dict(overture_name=cat["overture_name"], google_name=cat["google_name"],
            overture_address=cat["overture_address"], google_address=cat["google_address"],
            google_maps_url=observed_url(g.get("source_url")) if g else "",
            overture_categories_raw=cat["overture_categories_raw"], google_categories_raw=cat["google_categories_raw"],
            overture_categories_normalized=cat["overture_categories"], google_categories_normalized=cat["google_categories"],
            taxonomy_hierarchy=raw.get("taxonomy_hierarchy"), distance_m=e["distance_m"] if e else None,
            name_similarity=e["name_similarity"] if e else None, er_classification=e["classification"] if e else None,
            category_relation=cat["category_relation"], category_comparison_status=cat["comparison_status"],
            shared_google_identity=cat["shared_google_identity"], multiple_google_candidates=cat["multiple_google_candidates"],
            baseline_fnb_candidate=cat["baseline_fnb_candidate"], overture_inside_ward=cat["overture_inside_ward"],
            reason_codes=sorted(set(cat["reason_codes"] + er_reasons)),
            er_reason=e["reason"].split(" V3 actual support/conflict/unknown features=", 1)[0] if e else None,
            category_reason=cat["reason"], er_evidence_json=compact if e else None,
            source_evidence_json=source_evidence, er_observation_ref=context["refs"]["er"] | {"key": dict(zip(KEY, k))} if e else None,
            **dict(zip(KEY, k)), overture_source_release=json.loads(ov["source_provenance_json"]).get("source_release") if ov else None,
            **{f: context[f] for f in ("google_silver_release_id", "er_release_id", "category_release_id")},
            er_rule_version=e["rule_version"] if e else None, category_rule_version=cat["rule_version"],
            category_mapping_version=cat["mapping_version"], review_schema_version=VERSION,
            er_evidence_hash=_digest(json.loads(e["evidence_json"])) if e else None,
            category_evidence_hash=_digest(json.loads(cat["evidence_json"])), source_evidence_hash=_digest(source_evidence),
            source_provenance_json=cat["source_provenance_json"], input_refs_json=context["refs"])
        for queue, needed, output, labels in (("identity", identity_needed, identities, IDENTITY_LABELS), ("category", category_needed, categories, CATEGORY_LABELS)):
            if not needed:
                continue
            grain = "LINK" if linked else "OVERTURE_SOURCE_ONLY"
            row = dict(base, review_id=queue + "_" + _digest(dict(grain=grain, key=list(k))), review_grain=grain)
            if queue == "identity":
                _, row["priority"], row["priority_reason"] = identity_priority(cat, e, current_conflict)
            elif cat["category_relation"] == "CATEGORY_CONFLICT":
                row.update(priority="P1_TRUE_CATEGORY_CONFLICT", priority_reason="Two linked sources disagree on F&B/non-F&B; resolve identity and then business category.")
            else:
                row.update(priority="P2_TAXONOMY_NAME_WARNING", priority_reason="Food-related name but excluded taxonomy; name is only an inspection hint, not a verified category.")
            row = strings(row)
            # A future importer must re-read the pinned sources and compare these
            # hashes. Excel may reformat distance cells; display formatting is not evidence.
            row["evidence_hash"] = _digest({f: row[f] for f in ("review_id", "review_grain", *KEY,
                "overture_source_release", "google_silver_release_id", "er_release_id", "category_release_id",
                "er_rule_version", "category_rule_version", "category_mapping_version", "review_schema_version",
                "er_evidence_hash", "category_evidence_hash", "source_evidence_hash", "input_refs_json")})
            row.update({f: "" for f in labels})
            require(all(len(v) <= 32767 for v in row.values()), "Review cell exceeds Excel limit; use a narrower evidence projection")
            output.append(row)
    for rows in (identities, categories):
        rows.sort(key=lambda r: (r["priority"], r["overture_name"].casefold(), r["google_name"].casefold(), r["review_id"]))
        require(len({r["review_id"] for r in rows}) == len(rows), "Duplicate review ID")
        require(len({key(r) for r in rows}) == len(rows), "Duplicate review composite key")
    expected_identity = {k for k, r in er.items() if r["classification"] == "REVIEW"} | {k for k, r in cats.items() if all(k) and (r["category_relation"] == "CATEGORY_CONFLICT" or r["comparison_status"] == "NEEDS_IDENTITY_RECHECK")}
    expected_category = {k for k, r in cats.items() if r["category_relation"] == "CATEGORY_CONFLICT" or "OVERTURE_FNB_NAME_HINT_TAXONOMY_EXCLUDED" in r["reason_codes"]}
    require({key(r) for r in identities} == expected_identity, "Identity queue coverage loss")
    require({key(r) for r in categories} == {tuple(v or "" for v in k) for k in expected_category}, "Category queue coverage loss")
    summary = dict(review_schema_version=VERSION, er_release_id=context["er_release_id"], category_release_id=context["category_release_id"],
        google_silver_release_id=context["google_silver_release_id"], inputs=context["refs"],
        identity=dict(rows=len(identities), er_review_links=sum(r["classification"] == "REVIEW" for r in er.values()),
            additional_links=sum(r["er_classification"] != "REVIEW" for r in identities),
            priorities=dict(sorted(Counter(r["priority"] for r in identities).items())),
            valid_observed_urls=sum(bool(r["google_maps_url"]) for r in identities),
            shared_id_links=sum(r["shared_google_identity"] == "true" for r in identities),
            multiple_candidate_links=sum(r["multiple_google_candidates"] == "true" for r in identities)),
        category=dict(rows=len(categories), category_conflicts=sum(r["category_relation"] == "CATEGORY_CONFLICT" for r in categories),
            taxonomy_name_warnings=sum("OVERTURE_FNB_NAME_HINT_TAXONOMY_EXCLUDED" in json.loads(r["reason_codes"]) for r in categories),
            grains=dict(sorted(Counter(r["review_grain"] for r in categories).items())),
            valid_observed_urls=sum(bool(r["google_maps_url"]) for r in categories)),
        labels=dict(human_label=["MATCH", "NON_MATCH", "UNSURE"], reviewed_fnb_status=["FNB", "NON_FNB", "UNSURE"],
            reviewed_category=["restaurant", "cafe_coffee", "beverage", "bakery_dessert", "fast_food", "bar", "other_food", "non_fnb", "unknown"]),
        instructions=["Edit only the blank annotation columns; preserve IDs, releases and evidence hashes.",
            "A category decision is not an identity MATCH. No label is prefilled or imported.",
            "reviewed_category accepts one documented group, or blank when unsure; use reviewer_note for multiple or ambiguous categories.",
            "Evidence hash pins stable identity, releases, rules and canonical source evidence hashes; Excel numeric display formatting is excluded.",
            "Full ER/category evidence remains at input_refs_json paths, addressed by the composite source key.",
            "Missing URL means no trusted observed URL; no URL was guessed from a Google data_id."])
    return identities, categories, summary


def load(er_root, category_root):
    roots = [Path(r).resolve() for r in (er_root, category_root)]
    er_dir, cat_dir = [silver.read_active(r) for r in roots]
    require(er_dir is not None and cat_dir is not None, "Published ER and category releases required")
    em, cm = [silver.verify_release(d) for d in (er_dir, cat_dir)]
    require(em["signature"].get("rule_version") == "ov-google-er-proposal-v3-experimental", "Expected ER V3")
    require(cm["signature"].get("rule_version") == "category-quality-v1", "Expected Category Quality V1")
    erp, catp, normp = er_dir / "er_results.parquet", cat_dir / "category_audit.parquet", cat_dir / "category_normalization.parquet"
    require(cm["signature"]["inputs_sha256"].get(str(erp)) == silver.sha(erp), "Category release does not reference active ER evidence")
    pp = [Path(p) for p in cm["signature"]["inputs_sha256"] if Path(p).name == "google_places.parquet"]
    require(len(pp) == 1, "Cannot identify pinned Google source")
    places = pp[0]
    require(silver.sha(places) == cm["signature"]["inputs_sha256"][str(places)], "Pinned Google checksum mismatch")
    google_dir = places.parent.parent
    gm = silver.verify_release(google_dir)
    require(em["signature"]["inputs_sha256"].get(str(places)) == silver.sha(places), "Google/ER lineage mismatch")
    paths = [erp, catp, normp, places, er_dir / "manifest.json", cat_dir / "manifest.json", google_dir / "manifest.json", *[r / "current.json" for r in roots]]
    hashes = {str(p.resolve()): silver.sha(p) for p in paths}
    refs = {name: dict(path=str(p.resolve()), sha256=hashes[str(p.resolve())]) for name, p in (("er", erp), ("category_audit", catp), ("category_normalization", normp), ("google_places", places))}
    context = dict(er_release_id=em["release_id"], category_release_id=cm["release_id"], google_silver_release_id=gm["release_id"], refs=refs)
    er_rows = pq.read_table(erp).to_pylist()
    category_rows = pq.read_table(catp).to_pylist()
    normal_cols = ["source", "source_id", "google_id_kind", "google_identity_value", "raw_categories_json", "normalized_categories", "category_evidence_json", "dq_flags", "source_provenance_json"]
    normal = pq.read_table(normp, columns=normal_cols).to_pylist()
    google = pq.read_table(places, columns=["google_id_kind", "google_identity_value", "source_url"]).to_pylist()
    return (er_rows, category_rows, normal, google, context), hashes, roots + [google_dir]


def export(er_root, category_root, output_root):
    data, hashes, roots = load(er_root, category_root)
    output = Path(output_root).resolve()
    require(all(not output.is_relative_to(r) and not r.is_relative_to(output) for r in roots), "Separate input/output directories required")
    identities, categories, summary = build(*data)
    summary["input_hashes"] = hashes
    signature = dict(version=VERSION, input_hashes=hashes, code_sha256=silver.sha(__file__))
    export_id = _digest(signature)[:24]
    output.mkdir(parents=True, exist_ok=True)
    stage = output / (".stage-" + uuid.uuid4().hex)
    stage.mkdir()
    try:
        artifacts = {}
        for name, rows, fields in (("identity_review.csv", identities, IDENTITY_FIELDS), ("category_review.csv", categories, CATEGORY_FIELDS)):
            p = stage / name
            with p.open("x", encoding="utf-8-sig", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=fields)
                writer.writeheader()
                writer.writerows(rows)
            with p.open(encoding="utf-8-sig", newline="") as stream:
                require(list(csv.DictReader(stream)) == rows, "CSV round-trip altered values")
            require(p.read_bytes().startswith(b"\xef\xbb\xbf"), "CSV lacks UTF-8 BOM")
            artifacts[name] = dict(rows=len(rows), sha256=silver.sha(p))
        summary.update(export_id=export_id, artifacts=artifacts, export_signature=signature)
        (stage / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        for p, digest in hashes.items():
            require(silver.sha(p) == digest, "Input changed during export: " + p)
        # Always create a fresh directory, even if old files look unlabelled.
        # mkdir is exclusive; no existing labels, notes or prior exports are overwritten.
        suffix = 1
        while True:
            destination = output / (export_id if suffix == 1 else f"{export_id}-{suffix:03d}")
            try:
                destination.mkdir()
                break
            except FileExistsError:
                suffix += 1
        for p in stage.iterdir():
            os.replace(p, destination / p.name)
        stage.rmdir()
        return destination, summary
    finally:
        if stage.exists():
            require(stage.resolve().parent == output and stage.name.startswith(".stage-"), "Unsafe export stage cleanup")
            shutil.rmtree(stage)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--er-root", type=Path, required=True)
    parser.add_argument("--category-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    path, summary = export(args.er_root, args.category_root, args.output_root)
    print(silver.dump(dict(output=str(path), identity=summary["identity"], category=summary["category"])))


if __name__ == "__main__":
    main()
