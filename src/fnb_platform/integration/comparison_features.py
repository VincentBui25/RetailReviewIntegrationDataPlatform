"""M1.5: offline name/address features, not ER matching or decisions."""
from __future__ import annotations

import argparse
import json
import os
import re
import unicodedata
import uuid
from collections import Counter
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .. import google_silver as silver
from ..overture_incremental import _digest

NORMALIZATION_VERSION = "vn-comparison-text-v1"
ADDRESS_VERSION = "vn-address-components-v1"
PAIR_VERSION = "address-component-evidence-v1"
HOUSE = r"\d+[A-Za-z]?(?:/\d+[A-Za-z]?)*"
STREET_PREFIX = r"(?:đường|duong|đ\.|d\.|street)\s*"
ALLEY_PREFIX = r"(?:hẻm|hem|ngõ|ngo|ngách|ngach|kiệt|kiet)\s*"
QUALIFIER = r"(?:trước cửa nhà|truoc cua nha|đối diện|doi dien|gần|gan)\s+"
ADMIN = r"(?:(?:phường|phuong|quận|quan|ward|district|thành phố|thanh pho|tỉnh|tinh|huyện|huyen|xã|xa)\b|[pq]\.?\s*\d+\b|[pq]\.(?=\s)|tp\.(?=\s))"
PARSE_STATUSES = {"PARSED", "PARTIAL", "AMBIGUOUS", "MISSING"}
ADDRESS_FIELDS = [
    ("address_raw", pa.string()), ("address_comparison", pa.string()),
    ("house_identifier", pa.string()), ("house_number", pa.string()),
    ("house_suffix", pa.string()), ("house_path", pa.list_(pa.string())),
    ("house_range_raw", pa.string()), ("house_range_start", pa.string()),
    ("house_range_end", pa.string()), ("house_is_reference", pa.bool_()),
    ("alley_raw", pa.list_(pa.string())), ("alley_path", pa.list_(pa.string())),
    ("street_raw", pa.string()), ("street_comparison", pa.string()),
    ("premise_context", pa.list_(pa.string())), ("unparsed_parts", pa.list_(pa.string())),
    ("parse_status", pa.string()), ("parse_notes", pa.list_(pa.string())),
]
FEATURE_FIELDS = [
    ("name_raw", pa.string()), ("name_existing_normalized", pa.string()),
    ("name_comparison", pa.string()), ("address_existing_normalized", pa.string()),
    *ADDRESS_FIELDS,
    ("name_source_ref_json", pa.string()), ("address_source_ref_json", pa.string()),
    ("normalization_version", pa.string()), ("address_rule_version", pa.string()),
]
OV_SCHEMA = pa.schema([("overture_id", pa.string()), *FEATURE_FIELDS])
GG_SCHEMA = pa.schema([("google_id_kind", pa.string()), ("google_identity_value", pa.string()), *FEATURE_FIELDS])
PAIR_SCHEMA = pa.schema([
    ("overture_id", pa.string()), ("google_id_kind", pa.string()), ("google_identity_value", pa.string()),
    ("overture_name_comparison", pa.string()), ("google_name_comparison", pa.string()),
    ("house_component_relation", pa.string()), ("street_component_relation", pa.string()),
    ("alley_component_relation", pa.string()), ("address_evidence_json", pa.string()),
    ("historical_m1_dq_flags", pa.list_(pa.string())),
    ("historical_address_flags", pa.list_(pa.string())),
    ("historical_source_observation_refs", pa.list_(pa.string())),
    ("historical_selection_policies", pa.list_(pa.string())),
    ("historical_flag_usage", pa.string()), ("m1_source_link_ref_json", pa.string()),
    ("normalization_version", pa.string()), ("address_rule_version", pa.string()),
    ("comparison_rule_version", pa.string()),
])


def comparison_text(value):
    """One policy for both sources: accents/case/space, without deleting tokens."""
    if not isinstance(value, str) or not value.strip():
        return None
    value = unicodedata.normalize("NFKD", unicodedata.normalize("NFKC", value).casefold().replace("đ", "d"))
    value = "".join(c for c in value if not unicodedata.combining(c))
    # Keep brand punctuation and numeric branch paths (H103, 12/3, ABC-01).
    value = "".join(" " if unicodedata.category(c).startswith("C") else c for c in value)
    return " ".join(value.split()) or None


def visible_text(value):
    value = unicodedata.normalize("NFC", unicodedata.normalize("NFKC", value))
    return " ".join("".join(" " if unicodedata.category(c).startswith("C") else c for c in value).split())


def parse_address(value):
    """Conservative components; NULL and uncertain parsing are different states."""
    result = dict(address_raw=value, address_comparison=comparison_text(value), house_identifier=None,
        house_number=None, house_suffix=None, house_path=[], house_range_raw=None,
        house_range_start=None, house_range_end=None, house_is_reference=False,
        alley_raw=[], alley_path=[], street_raw=None, street_comparison=None,
        premise_context=[], unparsed_parts=[], parse_status="MISSING", parse_notes=[])
    if value is None or isinstance(value, str) and not value.strip():
        result["parse_notes"] = ["ADDRESS_NOT_PROVIDED"]
        return result
    if not isinstance(value, str):
        result.update(parse_status="AMBIGUOUS", parse_notes=["NON_STRING_ADDRESS"])
        return result
    clean = visible_text(value)
    if not clean:
        result.update(parse_status="AMBIGUOUS", parse_notes=["NO_ADDRESS_TEXT_AFTER_UI_GLYPHS"])
        return result
    parts = [p.strip() for p in re.split(r",|/(?=\s*" + ALLEY_PREFIX + r"\d)", clean, flags=re.I) if p.strip()]
    streets = []
    ambiguous = False
    seen_house = False
    for index, original in enumerate(parts):
        if not original:
            continue
        part = original
        part = re.sub(r"(?<=[0-9A-Za-z])\s*/\s*(?=\d)", "/", part)
        if re.match(r"^" + ADMIN, part, re.I):
            result["unparsed_parts"].append(original)
            continue
        # Administrative labels attached after the street are not street tokens.
        part = re.split(r"\s+(?:-\s*)?(?=" + ADMIN + r")", part, maxsplit=1, flags=re.I)[0].strip()
        qualifier = re.match(r"^" + QUALIFIER, part, re.I)
        if qualifier:
            result["house_is_reference"] = True
            result["parse_notes"].append("HOUSE_IS_LOCATION_REFERENCE_NOT_CONFIRMED_DOOR")
            ambiguous = True
            part = part[qualifier.end():]
        alley_inside = re.search(r"\b" + ALLEY_PREFIX + HOUSE, part, re.I)
        premise = re.search(r"\b(?:cư xá|cu xa|tttm|chung cư|chung cu|tòa nhà|toa nha|lô|lo|block)\b", part, re.I)
        if comparison_text(part) in {"mat tien", "dia chi", "address"}:
            result["premise_context"].append(original)
            continue
        if premise and not re.match(r"^" + HOUSE + r"(?:\s|$)", part):
            if alley_inside:
                result["premise_context"].append(part[:alley_inside.start()].strip())
                part = part[alley_inside.start():]
            else:
                result["premise_context"].append(original)
                continue
        # A bare range before a comma belongs to the following street segment.
        range_match = re.match(r"^(\d+[A-Za-z]?)\s*[-–—]\s*(\d+[A-Za-z]?)(?=\s|$)", part)
        house_match = re.match(r"^(" + HOUSE + r")(?=\s|$)", part) if not range_match else None
        has_locator = bool(range_match or house_match)
        if has_locator:
            if seen_house:
                result["parse_notes"].append("MULTIPLE_HOUSE_REFERENCES")
                ambiguous = True
            else:
                seen_house = True
                if range_match:
                    result.update(house_range_raw=range_match.group(0),
                        house_range_start=range_match.group(1).casefold(),
                        house_range_end=range_match.group(2).casefold())
                    start, end = result["house_range_start"], result["house_range_end"]
                    if not start.isdigit() or not end.isdigit() or int(start) > int(end):
                        ambiguous = True
                        result["parse_notes"].append("RANGE_ORDER_OR_SUFFIX_UNDETERMINED")
                else:
                    identifier = house_match.group(1).casefold()
                    base = re.fullmatch(r"(\d+)([a-z]?)", identifier.split("/")[0])
                    result.update(house_identifier=identifier, house_number=base.group(1),
                        house_suffix=base.group(2) or None, house_path=identifier.split("/"))
            end = range_match.end() if range_match else house_match.end()
            part = part[end:].strip()
            if not part and index + 1 < len(parts) and not re.match(r"^" + ADMIN, parts[index + 1], re.I):
                part = parts[index + 1]
                parts[index + 1] = ""
        alley = re.match(r"^" + ALLEY_PREFIX + r"(" + HOUSE + r")(?=\s|$)", part, re.I)
        if alley:
            result["alley_raw"].append(alley.group(0))
            result["alley_path"].extend(alley.group(1).casefold().split("/"))
            part = part[alley.end():].strip()
            if result["house_path"] and result["house_path"][0] != result["alley_path"][0]:
                ambiguous = True
                result["parse_notes"].append("HOUSE_PATH_AND_EXPLICIT_ALLEY_HAVE_DIFFERENT_ROOTS")
        prefix = re.match(r"^" + STREET_PREFIX, part, re.I)
        if prefix:
            part = part[prefix.end():].strip()
        if re.match(r"^(?:lô\s|lo\s|cư xá\b|cu xa\b|chung cư\b|chung cu\b|tttm\b)", part, re.I):
            # A lot/apartment locator is not a street. Preserve it and leave
            # a later unprefixed label uncertain rather than invent a road.
            result["premise_context"].append(part)
            result["parse_notes"].append("PREMISE_LOCATOR_NOT_ASSUMED_TO_BE_STREET")
            continue
        identifiable_street = bool(part and (has_locator or alley or prefix))
        if identifiable_street:
            if re.match(r"^(?:và|va|hoặc|hoac|&|[-–—])\s*\d", part, re.I) or ((has_locator or alley) and not prefix and re.match(r"^" + HOUSE + r"(?:\s|$)", part)):
                ambiguous = True
                result["unparsed_parts"].append(part)
                result["parse_notes"].append("MULTIPLE_OR_UNSUPPORTED_NUMERIC_REFERENCES")
            elif not re.match(r"^" + ADMIN, part, re.I):
                streets.append(part)
        elif part and result["premise_context"] and index > 0 and not re.match(r"^" + ADMIN, part, re.I):
            # A bare label after a premise could also be a locality; do not guess.
            result["unparsed_parts"].append(original)
            result["parse_notes"].append("BARE_TEXT_AFTER_PREMISE_UNDETERMINED")
        elif part:
            result["unparsed_parts"].append(original)
    street_values = {comparison_text(s): s for s in streets}
    if len(street_values) == 1:
        result["street_comparison"], result["street_raw"] = next(iter(street_values.items()))
    elif len(street_values) > 1:
        ambiguous = True
        result["parse_notes"].append("MULTIPLE_DISTINCT_STREET_REFERENCES")
        result["unparsed_parts"].extend(streets)
    has_house = bool(result["house_identifier"] or result["house_range_start"])
    if not has_house:
        result["parse_notes"].append("HOUSE_NOT_IDENTIFIED")
    if not result["street_comparison"]:
        result["parse_notes"].append("STREET_NOT_IDENTIFIED")
    result["parse_status"] = ("AMBIGUOUS" if ambiguous else "PARSED" if has_house and result["street_comparison"]
        else "PARTIAL" if has_house or result["alley_path"] or result["street_comparison"] or result["premise_context"]
        else "AMBIGUOUS")
    result["parse_notes"] = sorted(set(result["parse_notes"]))
    return result


def house_relation(left, right):
    if left["house_is_reference"] or right["house_is_reference"]:
        return "UNDETERMINED_REFERENCE"
    uncertain = {"MULTIPLE_HOUSE_REFERENCES", "RANGE_ORDER_OR_SUFFIX_UNDETERMINED",
                 "MULTIPLE_OR_UNSUPPORTED_NUMERIC_REFERENCES", "HOUSE_PATH_AND_EXPLICIT_ALLEY_HAVE_DIFFERENT_ROOTS"}
    if uncertain.intersection(left["parse_notes"] + right["parse_notes"]):
        return "UNDETERMINED"
    a, b = left["house_identifier"], right["house_identifier"]
    ar, br = left["house_range_start"], right["house_range_start"]
    if a and b:
        if a == b:
            return "SAME_LITERAL"
        if "/" in a and "/" in b and (a.endswith("/" + b) or b.endswith("/" + a)):
            return "HIERARCHICAL_SUFFIX_OVERLAP"
        return "DIFFERENT_LITERAL"
    if ar and br:
        a1, a2 = int(ar), int(left["house_range_end"])
        b1, b2 = int(br), int(right["house_range_end"])
        return "SAME_RANGE_LITERAL" if (a1, a2) == (b1, b2) else "OVERLAPPING_NUMERIC_RANGES" if max(a1, b1) <= min(a2, b2) else "DISJOINT_NUMERIC_RANGES"
    if ar and b or br and a:
        ranged, literal = (left, b) if ar else (right, a)
        if not literal.isdigit():
            return "UNDETERMINED"
        return "RANGE_CONTAINS_LITERAL" if int(ranged["house_range_start"]) <= int(literal) <= int(ranged["house_range_end"]) else "RANGE_EXCLUDES_LITERAL"
    return "MISSING_COMPONENT"


def component_relation(a, b):
    if a is None or b is None or a == [] or b == []:
        return "MISSING_COMPONENT"
    return "SAME_LITERAL" if a == b else "DIFFERENT_LITERAL"


def parquet_ref(path, digest, key, field=None):
    return dict(parquet_path=str(path), parquet_sha256=digest, key=key, field=field)


def make_feature(raw, name_ref, address_ref):
    return dict(name_raw=raw["name"], name_existing_normalized=raw.get("name_normalized"),
        name_comparison=comparison_text(raw["name"]),
        address_existing_normalized=raw.get("address_normalized"), **parse_address(raw["address"]),
        name_source_ref_json=silver.dump(name_ref), address_source_ref_json=silver.dump(address_ref),
        normalization_version=NORMALIZATION_VERSION, address_rule_version=ADDRESS_VERSION)


def build(overture, places, links, refs):
    ov = {r["source_id"]: r for r in overture.to_pylist()}
    gg = {(r["google_id_kind"], r["google_identity_value"]): r for r in places.to_pylist()}
    if len(ov) != overture.num_rows or len(gg) != places.num_rows:
        raise ValueError("Duplicate source feature input key")
    source_ids = sorted({l["overture_id"] for l in links.to_pylist()})
    if set(source_ids) - ov.keys():
        raise ValueError("Source links refer to Overture IDs absent from pinned Silver")
    ov_features, gg_features = {}, {}
    for source in source_ids:
        r = ov[source]
        name_ref = parquet_ref(*refs["overture"], dict(source_id=source), "name")
        address_ref = parquet_ref(*refs["overture"], dict(source_id=source), "address")
        ov_features[source] = dict(overture_id=source, **make_feature(r, name_ref, address_ref))
    for key, r in sorted(gg.items()):
        provenance = json.loads(r["field_provenance_json"])
        source_refs = {}
        for field in ("name", "address"):
            raw_ref = provenance.get(field + "_display")
            source_refs[field] = dict(silver_reference=parquet_ref(*refs["places"],
                dict(google_id_kind=key[0], google_identity_value=key[1]), field + "_display"),
                raw_observation_reference=raw_ref,
                value_missing=r[field + "_display"] is None)
        raw = dict(name=r["name_display"], name_normalized=r["name_normalized"],
                   address=r["address_display"], address_normalized=r["address_normalized"])
        gg_features[key] = dict(google_id_kind=key[0], google_identity_value=key[1],
                               **make_feature(raw, source_refs["name"], source_refs["address"]))
    pairs = []
    for link in sorted(links.to_pylist(), key=lambda l: (l["overture_id"], l["google_id_kind"], l["google_identity_value"])):
        key = link["google_id_kind"], link["google_identity_value"]
        if key not in gg_features:
            raise ValueError("Google feature FK missing")
        left, right = ov_features[link["overture_id"]], gg_features[key]
        house = house_relation(left, right)
        street = component_relation(left["street_comparison"], right["street_comparison"])
        alley = component_relation(left["alley_path"], right["alley_path"])
        evidence = dict(overture={k: left[k] for k, _ in ADDRESS_FIELDS},
            google={k: right[k] for k, _ in ADDRESS_FIELDS},
            source_refs=dict(overture=json.loads(left["address_source_ref_json"]), google=json.loads(right["address_source_ref_json"])),
            house_comparison=dict(relation=house, overture_literal=left["house_identifier"],
                google_literal=right["house_identifier"], overture_range=[left["house_range_start"], left["house_range_end"]],
                google_range=[right["house_range_start"], right["house_range_end"]]),
            street_comparison=dict(relation=street, overture=left["street_comparison"], google=right["street_comparison"]),
            alley_comparison=dict(relation=alley, overture=left["alley_path"], google=right["alley_path"]),
            missing_policy="Missing/ambiguous components do not establish inequality; component relations are not ER decisions.",
            normalization_version=NORMALIZATION_VERSION, address_rule_version=ADDRESS_VERSION, rule_version=PAIR_VERSION)
        pairs.append(dict(overture_id=link["overture_id"], google_id_kind=key[0], google_identity_value=key[1],
            overture_name_comparison=left["name_comparison"], google_name_comparison=right["name_comparison"],
            house_component_relation=house, street_component_relation=street, alley_component_relation=alley,
            address_evidence_json=silver.dump(evidence), historical_m1_dq_flags=link["dq_flags"],
            historical_address_flags=[f for f in link["dq_flags"] if f in {"HOUSE_CONFLICT", "STREET_CONFLICT"}],
            historical_source_observation_refs=link["source_observation_refs"],
            historical_selection_policies=link["selection_policies"],
            historical_flag_usage="AUDIT_ONLY_NOT_CURRENT_ER_CONCLUSIONS",
            m1_source_link_ref_json=silver.dump(parquet_ref(*refs["links"], dict(overture_id=link["overture_id"], google_id_kind=key[0], google_identity_value=key[1]))),
            normalization_version=NORMALIZATION_VERSION, address_rule_version=ADDRESS_VERSION, comparison_rule_version=PAIR_VERSION))
    tables = {"comparison/overture_features.parquet": pa.Table.from_pylist(list(ov_features.values()), schema=OV_SCHEMA),
              "comparison/google_features.parquet": pa.Table.from_pylist(list(gg_features.values()), schema=GG_SCHEMA),
              "comparison/address_pair_evidence.parquet": pa.Table.from_pylist(pairs, schema=PAIR_SCHEMA)}
    for table in tables.values():
        if "parse_status" in table.column_names and set(table["parse_status"].to_pylist()) - PARSE_STATUSES:
            raise ValueError("Unexpected parse status")
    if len({(p["overture_id"], p["google_id_kind"], p["google_identity_value"]) for p in pairs}) != len(pairs):
        raise ValueError("Duplicate comparison key")
    if len(pairs) != links.num_rows:
        raise ValueError("Lost M1 relationships")
    summary = dict(normalization_version=NORMALIZATION_VERSION, address_rule_version=ADDRESS_VERSION,
        comparison_rule_version=PAIR_VERSION, overture_feature_rows=len(ov_features), google_feature_rows=len(gg_features),
        pair_rows=len(pairs), overture_parse_status=dict(sorted(Counter(r["parse_status"] for r in ov_features.values()).items())),
        google_parse_status=dict(sorted(Counter(r["parse_status"] for r in gg_features.values()).items())),
        house_component_relations=dict(sorted(Counter(p["house_component_relation"] for p in pairs).items())),
        street_component_relations=dict(sorted(Counter(p["street_component_relation"] for p in pairs).items())),
        historical_address_flag_rows=sum(bool(p["historical_address_flags"]) for p in pairs),
        scope="Comparison features only; no scoring, ER decisions, matching, merge, review queue, review processing or Gold.",
        coverage="Overture features cover linked IDs only; zero-candidate source IDs remain in unchanged M1 inventory.")
    return tables, summary


def publish(overture_silver, google_silver_root, output_root, *, fault=lambda stage: None):
    overture_silver = Path(overture_silver).resolve()
    google_root, output = Path(google_silver_root).resolve(), Path(output_root).resolve()
    if output.is_relative_to(google_root) or google_root.is_relative_to(output) or output.is_relative_to(overture_silver.parent) or overture_silver.parent.is_relative_to(output):
        raise ValueError("Comparison output must be separate from published inputs")
    current = silver.read_active(google_root)
    if current is None:
        raise ValueError("Google Silver has no active release")
    manifest = json.loads((current / "manifest.json").read_text(encoding="utf-8"))
    files = dict(overture=overture_silver, places=current / "silver/google_places.parquet", links=current / "silver/source_links.parquet")
    hashes = {str(p): silver.sha(p) for p in [*files.values(), current / "manifest.json", google_root / "current.json"]}
    if manifest["signature"]["inputs_sha256"].get(str(overture_silver)) != hashes[str(overture_silver)]:
        raise ValueError("Overture Silver differs from M1 pinned input")
    refs = {k: (str(p), hashes[str(p)]) for k, p in files.items()}
    tables, summary = build(*(pq.ParquetFile(files[k]).read() for k in ("overture", "places", "links")), refs)
    # Verify the name/address provenance actually used, without reading reviews.
    for row in tables["comparison/google_features.parquet"].to_pylist():
        for field in ("name", "address"):
            ref = json.loads(row[field + "_source_ref_json"])["raw_observation_reference"]
            if ref:
                path = Path(ref["metadata_path"]).resolve()
                digest = silver.sha(path)
                if digest != ref["metadata_sha256"]:
                    raise ValueError("Raw field provenance checksum mismatch")
                raw = json.loads(path.read_text(encoding="utf-8"))
                if raw.get(field) != row[field + "_raw"]:
                    raise ValueError("Raw field provenance value mismatch")
                hashes[str(path)] = digest
    signature = dict(normalization_version=NORMALIZATION_VERSION, address_rule_version=ADDRESS_VERSION,
        comparison_rule_version=PAIR_VERSION, inputs_sha256=dict(sorted(hashes.items())),
        code_sha256={str(p.resolve()): silver.sha(p) for p in [Path(__file__), Path(silver.__file__), (Path(__file__).resolve().parents[1] / "overture_incremental.py")]},
        pyarrow_version=pa.__version__)
    release_id = _digest(signature)[:24]
    output.mkdir(parents=True, exist_ok=True)
    lock = output / ".publish.lock"
    with lock.open("x", encoding="utf-8") as stream:
        stream.write(str(os.getpid()))
    try:
        silver.read_active(output)
        destination = output / "releases" / release_id
        if destination.exists():
            existing = silver.verify_release(destination)
            if existing["signature"] != signature:
                raise ValueError("Existing comparison release signature mismatch")
        else:
            stage = output / (".stage-" + uuid.uuid4().hex)
            (stage / "comparison").mkdir(parents=True)
            (stage / "reports").mkdir()
            artifacts = {}
            for name, table in tables.items():
                pq.write_table(table, stage / name, compression="zstd")
                artifacts[name] = dict(sha256=silver.sha(stage / name), rows=table.num_rows,
                    schema=str(table.schema), logical_sha256=_digest(table.to_pylist()))
            name = "reports/normalization_summary.json"
            (stage / name).write_text(json.dumps(summary, sort_keys=True, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            artifacts[name] = dict(sha256=silver.sha(stage / name))
            (stage / "manifest.json").write_text(json.dumps(dict(release_id=release_id, signature=signature, artifacts=artifacts),
                ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
            silver.verify_release(stage)
            fault("after_stage_validation")
            for path, digest in hashes.items():
                if silver.sha(path) != digest:
                    raise ValueError("Input changed before comparison publication: " + path)
            destination.parent.mkdir(exist_ok=True)
            os.replace(stage, destination)
        fault("before_pointer_replace")
        pointer = output / (".current-" + uuid.uuid4().hex + ".tmp")
        pointer.write_text(json.dumps(dict(release_id=release_id, manifest_sha256=silver.sha(destination / "manifest.json")), sort_keys=True) + "\n", encoding="utf-8")
        os.replace(pointer, output / "current.json")
        return destination, summary
    finally:
        lock.unlink()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--overture-silver", type=Path, required=True)
    parser.add_argument("--google-silver-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args(argv)
    directory, summary = publish(args.overture_silver, args.google_silver_root, args.output_root)
    print(json.dumps(dict(release=str(directory), **summary), ensure_ascii=False))


if __name__ == "__main__":
    main()
