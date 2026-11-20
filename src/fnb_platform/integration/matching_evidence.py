"""Token and base corroboration evidence used by the canonical matching policy."""
import re
from ..comparison_features import HOUSE

DEFAULTS = dict(match_name_min=.90, token_dice_min=.85, match_distance_max_m=100.,
    single_token_distance_max_m=25., two_address_conflicts_min_m=200.,
    house_conflict_min_m=500., remote_street_min_m=2000.)
# Descriptors only determine whether overlap is distinctive; original tokens are
# never removed from the stored name, similarity inputs or branch evidence.
DESCRIPTORS = set("quan nha hang cafe coffee ca phe tra sua oc bun pho mi banh com chay beer food restaurant house bakery kitchen tea and seafood lau nuong duong so quan phuong chi nhanh ho chi minh".split())


def name_features(a, b, full_score):
    left, right = re.findall(r"[^\W_]+", a or ""), re.findall(r"[^\W_]+", b or "")
    sa, sb = set(left), set(right)
    shared = sa & sb
    smaller, larger = (left, right) if len(left) <= len(right) else (right, left)
    contiguous = bool(smaller) and any(larger[i:i+len(smaller)] == smaller for i in range(len(larger)-len(smaller)+1))
    containment = bool(sa and sb) and (sa <= sb or sb <= sa)
    distinctive = sorted(t for t in shared if t not in DESCRIPTORS and not t.isdigit())
    dice = 2*len(shared)/(len(sa)+len(sb)) if sa and sb else None
    numbers_a = sorted(t for t in sa if any(c.isdigit() for c in t))
    numbers_b = sorted(t for t in sb if any(c.isdigit() for c in t))
    branch_conflicts = []
    # Different numbers may be branches, house paths, or menu details: uncertain
    # name evidence blocks a positive proposal; it never proves NON_MATCH alone.
    if numbers_a and numbers_b and not set(numbers_a).intersection(numbers_b):
        branch_conflicts.append("DIFFERENT_NAME_NUMERIC_TOKENS")
    for label, pattern in [("DISTRICT", r"\b(?:quan|q)\s*(\d+)\b"),
                           ("BRANCH", r"\b(?:chi nhanh|cn)\s*(\d+)\b")]:
        va, vb = re.findall(pattern, a or ""), re.findall(pattern, b or "")
        if va and vb and set(va).isdisjoint(vb):
            branch_conflicts.append("DIFFERENT_EXPLICIT_"+label)
    full_supported = full_score is not None and full_score >= DEFAULTS["match_name_min"] and min(len(sa),len(sb)) >= 2
    normal_support = full_supported or bool(distinctive) and (
        containment and min(len(sa),len(sb)) >= 2
        or dice is not None and dice >= DEFAULTS["token_dice_min"])
    single_support = bool(distinctive) and contiguous and min(len(sa),len(sb)) == 1 and len(smaller[0]) >= 4
    return dict(full_similarity=full_score, token_dice=dice,
        token_containment=containment, contiguous_containment=contiguous,
        left_tokens=left, right_tokens=right, distinctive_shared_tokens=distinctive,
        left_numeric_tokens=numbers_a, right_numeric_tokens=numbers_b,
        branch_warning_codes=sorted(set(branch_conflicts)),
        primary_name_supported=bool(normal_support), single_token_anchor_supported=bool(single_support))


def evaluate_base_evidence(a, b, pair, score, distance, shared, multiple, identity_conflicts):
    nf = name_features(a["name_comparison"], b["name_comparison"], score)
    named_address_hints = {}
    for side, row in (("overture", a), ("google", b)):
        street = row["street_comparison"]
        hints = re.findall(r"(?<!\w)("+HOUSE+r")\s+(?:(?:duong|d\.)\s*)?"+re.escape(street)+r"\b",
                           row["name_comparison"] or "", flags=re.I) if street else []
        compatible = []
        for hint in hints:
            same = hint == row["house_identifier"]
            if hint.isdigit() and row["house_range_start"] and row["house_range_end"]:
                same = same or row["house_range_start"].isdigit() and row["house_range_end"].isdigit() and int(row["house_range_start"]) <= int(hint) <= int(row["house_range_end"])
            compatible.append(bool(same))
        named_address_hints[side] = dict(house_literals=hints, metadata_house=row["house_identifier"],
            metadata_range=[row["house_range_start"],row["house_range_end"]], street=street)
        if hints and row["parse_status"] == "PARSED" and not all(compatible):
            nf["branch_warning_codes"].append("NAME_ADDRESS_HINT_DIFFERS_FROM_"+side.upper()+"_ADDRESS")
    nf["branch_warning_codes"] = sorted(set(nf["branch_warning_codes"]))
    both_parsed = a["parse_status"] == b["parse_status"] == "PARSED"
    street_diff = pair["street_component_relation"] == "DIFFERENT_LITERAL"
    house_diff = pair["house_component_relation"] in {"DIFFERENT_LITERAL", "DISJOINT_NUMERIC_RANGES", "RANGE_EXCLUDES_LITERAL"}
    house_same = pair["house_component_relation"] in {"SAME_LITERAL", "SAME_RANGE_LITERAL", "RANGE_CONTAINS_LITERAL", "OVERLAPPING_NUMERIC_RANGES"}
    # Overlapping ranges do not identify a precise branch; do not use as strong support.
    house_same = house_same and pair["house_component_relation"] != "OVERLAPPING_NUMERIC_RANGES"
    strong_address = both_parsed and house_same and pair["street_component_relation"] == "SAME_LITERAL"
    # Administrative suffixes are legitimately retained as unparsed parts by M1.5.
    # For explicit-street PARTIAL, allow only the documented missing-house note;
    # never reinterpret an AMBIGUOUS address as a certain conflict.
    reliable_streets = all(r["street_comparison"] and r["parse_status"] in {"PARSED", "PARTIAL"}
        and not r["house_is_reference"] and set(r["parse_notes"]) <= {"HOUSE_NOT_IDENTIFIED"} for r in (a,b))
    context = dict(name_features=nf, named_address_hints=named_address_hints, strong_address_agreement=strong_address,
        fully_parsed_house_difference=both_parsed and house_diff,
        fully_parsed_street_difference=both_parsed and street_diff,
        tentative_component_difference=not both_parsed and (house_diff or street_diff),
        corroboratable_street_difference=reliable_streets and street_diff,
        shared_google_identity=shared, multiple_candidates=multiple)
    reasons = []
    if score is None or distance is None:
        status, rule = "INSUFFICIENT_DATA", "MISSING_PRIMARY_EVIDENCE"
    elif identity_conflicts:
        status, rule = "REVIEW", "IDENTITY_METADATA_CONFLICT"
        reasons.append("IDENTITY_METADATA_CONFLICT")
    elif (both_parsed and street_diff and house_diff and distance > DEFAULTS["two_address_conflicts_min_m"]
          or both_parsed and house_diff and distance > DEFAULTS["house_conflict_min_m"]
          or reliable_streets and street_diff and distance > DEFAULTS["remote_street_min_m"]):
        status, rule = "NON_MATCH_CANDIDATE", "CORROBORATED_WRONG_LOCATION"
    else:
        if multiple: reasons.append("MULTIPLE_GOOGLE_CANDIDATES")
        if nf["branch_warning_codes"]: reasons.append("NAME_BRANCH_WARNING")
        if both_parsed and (house_diff or street_diff): reasons.append("PARSED_ADDRESS_DIFFERENCE")
        if not both_parsed and (house_diff or street_diff): reasons.append("UNCERTAIN_ADDRESS_DIFFERENCE")
        if distance > DEFAULTS["match_distance_max_m"]: reasons.append("DISTANCE_OUTSIDE_MATCH_BAND")
        name_ok = nf["primary_name_supported"] or (nf["single_token_anchor_supported"]
            and strong_address and distance <= DEFAULTS["single_token_distance_max_m"])
        if not name_ok: reasons.append("NAME_NOT_DISTINCTIVE_OR_COMPATIBLE")
        full_supported = score >= DEFAULTS["match_name_min"] and nf["primary_name_supported"] and bool(nf["distinctive_shared_tokens"])
        if not strong_address and (shared or not full_supported): reasons.append("NEED_STRONG_ADDRESS_SUPPORT")
        if not reasons:
            status, rule = "MATCH_CANDIDATE", "NAME_ADDRESS_PROXIMITY" if strong_address else "FULL_NAME_PROXIMITY_NO_CONTRADICTION"
        else:
            status, rule = "REVIEW", "REVIEW_EVIDENCE_GAPS_OR_CONTRADICTIONS"
    context.update(applied_rule=rule, review_reason_codes=sorted(set(reasons)))
    return status, context
