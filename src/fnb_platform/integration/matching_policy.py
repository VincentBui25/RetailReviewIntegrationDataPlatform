"""Canonical SUPPORT/CONFLICT/UNKNOWN policy; release identifier stays immutable."""
import re
import json
import math

from . import matching_evidence as base_evidence
from ..comparison_features import comparison_text

VERSION = "ov-google-er-proposal-v3-experimental"
EVIDENCE_REQUIREMENTS = dict(policy_version=VERSION, required_v3_signals=["name", "address", "location"])
DEFAULTS = {**base_evidence.DEFAULTS, "typo_max_edits": 1, "typo_min_token_length": 4,
            "typo_anchor_distance_max_m": 25.}
LOCATOR = r"[a-z]?\d+[a-z0-9]*(?:/\d+[a-z0-9]*)*"
ADMIN = r"^(?:phuong\b|ward\b|quan\b|district\b|q\.?\s*\d|p\.?\s*\d|tp\b|thanh pho\b|tinh\b|ho chi minh\b|viet nam\b|vietnam\b)"
DESCRIPTORS = base_evidence.DESCRIPTORS | set("tiem co so san xuat dich vu ban lo am thuc cua keo kem bo vien hu tieu flan bbq hotpot snacks snack space spaces shop co ltd company organic delivery ngon lau nuong ca chao ech hai san ga mi y steak homemade coffee milktea establishment catering".split())
PHRASES = {"ca phe":"coffee", "cafe":"coffee", "caffe":"coffee", "cofffee":"coffee",
    "milk tea":"milktea", "tra sua":"milktea", "nha hang":"restaurant", "bbq":"nuong",
    "hotpot":"lau", "and":"", "tee":"tea"}


def name_text(raw):
    # Retain a numeric token in both versions of Pizza777/Pizza 777 and camel
    # case OcSu20k/Oc Su 20k. Original names and original full scores stay intact.
    raw = raw or ""
    spaced = "".join((" " if i and raw[i-1].islower() and c.isupper() else "")+c
                     for i,c in enumerate(raw))
    text = comparison_text(spaced) or ""
    text = re.sub(r"(?<=[^\W\d_])(?=\d)|(?<=\d)(?=[^\W\d_])", " ", text)
    return " ".join(re.findall(r"[^\W_]+",text))


def edits(a,b):
    if abs(len(a)-len(b))>1: return 2
    previous=list(range(len(b)+1))
    for i,x in enumerate(a,1):
        current=[i]
        for j,y in enumerate(b,1):current.append(min(current[-1]+1,previous[j]+1,previous[j-1]+(x!=y)))
        previous=current
    return previous[-1]


def surface(text):
    text=comparison_text(text)
    if not text:return None
    text=re.sub(r"\b(?:duong|street)\b|\bd\."," ",text)
    text=re.sub(r"\s*/\s*","/",text)
    text=re.sub(r"\s*[-–—]\s*","-",text)
    return " ".join(re.sub(r"[^\w/,-]+"," ",text).split()).strip(" ,")


def street_key(text):
    text=surface(text)
    if text and re.fullmatch(r"\d+[a-z]?",text):text="so "+text
    return text


def address_features(row):
    text=surface(row["address_raw"])
    parts=[p.strip() for p in (text or "").split(',') if p.strip()]
    street=street_key(row["street_comparison"])
    house=row["house_identifier"]
    recovery=None
    # Unambiguous observed leading alphanumeric locators (K62/R32A/183B6),
    # or street-first number syntax. These are derived features, not repaired raw.
    leading=re.match(r"^("+LOCATOR+r")\s+(.+)$",parts[0]) if parts else None
    if leading and not row["house_is_reference"]:
        tail=leading.group(2)
        if not re.search(r"\b(?:hem|ngo|ngach|kiet|lo|cu xa|chung cu)\b",tail):
            if house is None:house=leading.group(1);recovery="OBSERVED_LEADING_LOCATOR"
            if street is None:street=street_key(tail)
    raw=comparison_text(row["address_raw"]) or ""
    trailing=re.match(r"^(?:duong|d\.)\s+(.+?)\s+("+LOCATOR+r")$",raw.split(',')[0].strip())
    if trailing and not row["house_is_reference"]:
        house=trailing.group(2);street=street_key(trailing.group(1));recovery="EXPLICIT_STREET_THEN_NUMBER"
    # A slash hierarchy is retained in full. Explicit alleys/lots are independent
    # components, never collapsed to the final number or equated by proximity.
    alleys=re.findall(r"\b(?:hem|ngo|ngach|kiet)\s+("+LOCATOR+r")\b",text or "")
    lots=re.findall(r"\b(?:lo|block)\s+([a-z0-9]+)\b",text or "")
    locator_tokens=re.findall(r"\b[a-z]+\d+[a-z0-9]*\b",text or "")
    numeric_house=bool(house or row["house_range_start"])
    building_only=bool(parts and re.fullmatch(r"(?:chung cu|toa nha|cu xa)\s+[a-z]+\d+[a-z0-9]*",parts[0]))
    specific=bool(numeric_house or lots or locator_tokens) and not building_only
    multiple_streets=bool(row["street_comparison"] and re.search(r"\b(?:d\.|duong)\s+",row["street_comparison"]))
    return dict(surface=text,parts=parts,house=house,street=None if multiple_streets else street,
        lots=lots,alleys=alleys,locator_tokens=locator_tokens,specific_locator=specific,
        building_only=building_only,original_parse_status=row["parse_status"],original_parse_notes=row["parse_notes"],
        reference_only=row["house_is_reference"],recovery=recovery,multiple_street_text=multiple_streets)


def administrative_tail(parts):
    # The country/explicit admin marker is observed, not an inferred ward/city.
    # Unmarked locality suffixes are accepted only after a comma-delimited exact
    # identifying prefix, with no additional house/alley/lot/street locators.
    return bool(parts) and any(re.match(ADMIN,p) for p in parts) and all(
        not re.search(r"\b(?:hem|ngo|ngach|lo|block|chung cu|cu xa|duong)\b|\d",p)
        or bool(re.match(ADMIN,p)) for p in parts)


def address_comparison(a,b,pair):
    x,y=address_features(a),address_features(b)
    equal=bool(x["surface"] and x["surface"]==y["surface"])
    prefix_equal=False
    if not equal:
        for short,long in ((x,y),(y,x)):
            n=len(short["parts"])
            if n and short["parts"]==long["parts"][:n] and administrative_tail(long["parts"][n:]):prefix_equal=True
    observed_equal=equal or prefix_equal
    street_same=bool(x["street"] and x["street"]==y["street"])
    street_diff=bool(x["street"] and y["street"] and x["street"]!=y["street"])
    house_same=bool(x["house"] and x["house"]==y["house"])
    house_diff=bool(x["house"] and y["house"] and x["house"]!=y["house"])
    lot_diff=bool(x["lots"] and y["lots"] and x["lots"]!=y["lots"])
    alley_diff=bool(x["alleys"] and y["alleys"] and x["alleys"]!=y["alleys"])
    parsed=a["parse_status"]==b["parse_status"]=="PARSED"
    definite_house=parsed and pair["house_component_relation"] in {"DIFFERENT_LITERAL","DISJOINT_NUMERIC_RANGES","RANGE_EXCLUDES_LITERAL"}
    # Strictly recovered explicit locators may also expose a difference. Ambiguous
    # paths/reference expressions remain UNKNOWN; equality does not erase it.
    recovered_pair=all(r["original_parse_status"]=="PARSED" or r["recovery"] for r in (x,y))
    definite_house=bool(definite_house or recovered_pair and house_diff)
    definite_street=bool(parsed and street_diff and not (x["multiple_street_text"] or y["multiple_street_text"]))
    explicit_context=all(r["original_parse_status"] in {"PARSED","PARTIAL"} and not r["reference_only"]
        for r in (x,y))
    definite_lot=lot_diff and explicit_context and len(x["lots"])==len(y["lots"])==1
    definite_alley=alley_diff and explicit_context and len(x["alleys"])==len(y["alleys"])==1
    clear_conflict=definite_house or definite_street or definite_lot or definite_alley
    uncertain=bool(not observed_equal and (house_diff or street_diff or lot_diff or alley_diff) and not clear_conflict)
    component_agreement=street_same and (house_same or pair["house_component_relation"] in {"SAME_RANGE_LITERAL","RANGE_CONTAINS_LITERAL"}) and (parsed or recovered_pair)
    strong=bool((observed_equal or component_agreement) and not clear_conflict and not (x["reference_only"] or y["reference_only"]))
    return dict(overture=x,google=y,normalized_surface_equal=equal,
        observed_prefix_equal_with_admin_tail=prefix_equal,strong_agreement=strong,
        specific_agreement=strong and x["specific_locator"] and y["specific_locator"],
        house_state="CONFLICT" if definite_house else "SUPPORT" if house_same else "UNKNOWN",
        street_state="CONFLICT" if definite_street else "SUPPORT" if street_same else "UNKNOWN",
        lot_state="CONFLICT" if definite_lot else "SUPPORT" if x["lots"] and x["lots"]==y["lots"] else "UNKNOWN",
        alley_state="CONFLICT" if definite_alley else "SUPPORT" if x["alleys"] and x["alleys"]==y["alleys"] else "UNKNOWN",
        clear_conflict=bool(clear_conflict),uncertain_difference=uncertain,
        reference_only=x["reference_only"] or y["reference_only"])


def name_comparison(a,b,score,base):
    texts=[name_text(r["name_raw"]) for r in (a,b)]
    normalized=base_evidence.name_features(*texts,score)
    def core(row,text):
        canonical=text
        for old,new in sorted(PHRASES.items(),key=lambda p:-len(p[0])):
            canonical=re.sub(r"\b"+re.escape(old)+r"\b",new,canonical)
        # Locations are removed only from the additional core view when observed
        # in this side's address. Original tokens, numeric IDs and branch warnings
        # remain separately available and can veto a positive proposal.
        removed=[]
        street=name_text(row["street_comparison"])
        if street and re.search(r"\b"+re.escape(street)+r"\b",canonical):
            canonical=re.sub(r"\b"+re.escape(street)+r"\b"," ",canonical);removed.append(street)
        canonical=re.sub(r"\b(?:quan|q|district|phuong|ward|chi nhanh|cn)\s+\d+\b"," ",canonical)
        tokens=re.findall(r"[^\W_]+",canonical)
        # Non-Latin tokens stay in evidence; they are UNKNOWN translations and
        # do not count as contradictory Vietnamese/English brand tokens.
        brand=sorted(set(t for t in tokens if t not in DESCRIPTORS and t.isascii()))
        return dict(tokens=tokens,brand_tokens=brand,location_tokens_in_core_view=removed,
                    other_script_tokens=[t for t in tokens if not t.isascii()])
    cores=[core(r,t) for r,t in zip((a,b),texts)]
    heads=[core(r,name_text(re.split(r"\s[-|]\s|\(",r["name_raw"] or "",maxsplit=1)[0])) for r in (a,b)]
    head_tokens=[c["brand_tokens"] for c in heads]
    head_equal=bool(head_tokens[0]) and head_tokens[0]==head_tokens[1] and any(
        len(t)>=3 and not t.isdigit() for t in head_tokens[0])
    left,right=[c["brand_tokens"] for c in cores]
    exact_shared=sorted(set(left)&set(right))
    unmatched_left=sorted(set(left)-set(right));unmatched_right=sorted(set(right)-set(left))
    fuzzy=[]
    # Only one edit on a nonnumeric token of length >=4, with one-to-one alignment.
    # Do not approximate house/branch numbers or choose from ambiguous alignments.
    for x in unmatched_left:
        candidates=[y for y in unmatched_right if min(len(x),len(y))>=DEFAULTS["typo_min_token_length"]
            and not any(c.isdigit() for c in x+y) and edits(x,y)<=DEFAULTS["typo_max_edits"]]
        if len(candidates)==1 and sum(edits(z,candidates[0])<=1 for z in unmatched_left)==1:fuzzy.append([x,candidates[0]])
    aligned=len(exact_shared)+len(fuzzy)
    coverage=aligned/max(len(left),len(right)) if left and right else 0.
    core_equal=bool(left and right and len(left)==len(right)==aligned)
    # A single shared generic/numeric token never supports a match.
    distinctive=[t for t in exact_shared if not t.isdigit() and len(t)>=3]
    typo_support=bool(fuzzy) and core_equal
    alias_support=core_equal and bool(distinctive) and not fuzzy
    # Keep description containment already evidenced by V2; new partial shared
    # brands with conflicting suffixes do not qualify just by token coverage.
    warnings=sorted(set(normalized["branch_warning_codes"]+
        [w for w in base["name_features"]["branch_warning_codes"] if w!="DIFFERENT_NAME_NUMERIC_TOKENS"]))
    core_numbers=[sorted(t for t in c["brand_tokens"] if any(ch.isdigit() for ch in t)) for c in cores]
    if all(core_numbers) and core_numbers[0]!=core_numbers[1]:
        warnings=sorted(set(warnings+["DIFFERENT_CORE_NUMERIC_IDENTIFIERS"]))
    return dict(**normalized,comparison_texts=texts,core_views=cores,aligned_core_tokens=exact_shared,
        one_edit_alignments=fuzzy,core_coverage=coverage,core_equal=core_equal,
        descriptor_alias_support=alias_support,description_head_views=heads,
        descriptor_head_agreement=head_equal,typo_support=typo_support,
        core_numeric_identifiers=core_numbers,effective_branch_warning_codes=warnings)


def evaluate(a,b,pair,score,distance,shared,multiple,identity_conflicts):
    old_status,old=base_evidence.evaluate_base_evidence(a,b,pair,score,distance,shared,multiple,identity_conflicts)
    address=address_comparison(a,b,pair)
    names=name_comparison(a,b,score,old)
    warnings=names["effective_branch_warning_codes"]
    near=distance is not None and distance<=DEFAULTS["match_distance_max_m"]
    very_near=distance is not None and distance<=DEFAULTS["typo_anchor_distance_max_m"]
    name_support=(names["primary_name_supported"] or names["descriptor_alias_support"]
        or names["descriptor_head_agreement"] or old["name_features"]["primary_name_supported"])
    if names["typo_support"] and very_near:name_support=True
    if names["single_token_anchor_supported"] and very_near:name_support=True
    # Added name methods require specific corroboration; pure shared premises,
    # shared brand, missing address and short distance cannot establish identity.
    additional_support=bool(name_support and address["specific_agreement"] and near)
    reasons=[]
    if score is None or distance is None:
        status,rule="INSUFFICIENT_DATA","MISSING_PRIMARY_EVIDENCE"
    elif identity_conflicts:
        status,rule="REVIEW","IDENTITY_METADATA_CONFLICT";reasons.append("IDENTITY_METADATA_CONFLICT")
    elif old_status=="NON_MATCH_CANDIDATE" and not address["strong_agreement"]:
        status,rule="NON_MATCH_CANDIDATE","CORROBORATED_WRONG_LOCATION"
    else:
        if multiple:reasons.append("MULTIPLE_GOOGLE_CANDIDATES")
        if warnings:reasons.append("NAME_BRANCH_WARNING")
        if address["clear_conflict"]:reasons.append("CURRENT_COMPONENT_CONFLICT")
        if address["uncertain_difference"]:reasons.append("UNCERTAIN_ADDRESS_DIFFERENCE")
        if address["reference_only"]:reasons.append("REFERENCE_ONLY_ADDRESS")
        if not near:reasons.append("DISTANCE_OUTSIDE_MATCH_BAND")
        if not name_support:reasons.append("NAME_SUPPORT_UNKNOWN")
        legacy_positive=old_status=="MATCH_CANDIDATE"
        if not (additional_support or legacy_positive):reasons.append("NEED_SPECIFIC_ADDRESS_AND_NAME_SUPPORT")
        if not reasons:
            status,rule="MATCH_CANDIDATE","COMBINED_NAME_ADDRESS_LOCATION_SUPPORT"
        else:status,rule="REVIEW","CONFLICT_OR_INCOMPLETE_CORROBORATION"
    # Same interface as the prior policy; all new features remain separately
    # versioned and source-backed. Original M1.5 fields are untouched.
    context={**old,"name_features":names,"strong_address_agreement":address["strong_agreement"],
        "fully_parsed_house_difference":address["house_state"]=="CONFLICT",
        "fully_parsed_street_difference":address["street_state"]=="CONFLICT",
        "tentative_component_difference":address["uncertain_difference"],
        "address_features_v3":address,"v2_classification_recomputed":old_status,
        "applied_rule":rule,"review_reason_codes":sorted(set(reasons)),
        "signals":dict(name="CONFLICT" if warnings else "SUPPORT" if name_support else "UNKNOWN",
            address="CONFLICT" if address["clear_conflict"] else "SUPPORT" if address["strong_agreement"] else "UNKNOWN",
            location="SUPPORT" if near else "UNKNOWN",candidate="CONFLICT" if multiple or identity_conflicts else "UNKNOWN")}
    context["name_features"]["branch_warning_codes"]=warnings
    return status,context


def key(row):
    return tuple(row[f] for f in ("overture_id", "google_id_kind", "google_identity_value"))

def coordinate_valid(row):
    return all(isinstance(row.get(f), (int, float)) and not isinstance(row[f], bool) and math.isfinite(row[f]) and lo <= row[f] <= hi
        for f, lo, hi in (("latitude", -90, 90), ("longitude", -180, 180)))


def gates(er, cat, google, groups, current_human, policy):
    evidence = json.loads(er["evidence_json"])
    v3 = evidence.get("comparisons", {}).get("v3", {})
    signals = v3.get("signals", {})
    address = v3.get("address_features_v3", {})
    name = v3.get("name_features", {})
    conflicts, gaps = [], []
    google_group, source_group = groups
    if len(google_group) > 1:
        conflicts.append("SHARED_GOOGLE_ID")
    if len(source_group) > 1 and any(current_human[k] != "NON_MATCH" for k in source_group if k != key(er)):
        conflicts.append("MULTIPLE_GOOGLE_CANDIDATES_UNRESOLVED")
    if cat["category_relation"] == "CATEGORY_CONFLICT":
        conflicts.append("CATEGORY_FNB_NON_FNB_CONFLICT")
    source_flags = json.loads(cat["evidence_json"]).get("source_flags", {})
    if any(code in {"CONFLICTING_SOURCE_CATEGORIES", "FNB_HIERARCHY_NON_FNB_LABEL"} for codes in source_flags.values() for code in codes):
        conflicts.append("SOURCE_CATEGORY_CONFLICT")
    if address.get("clear_conflict") or signals.get("address") == "CONFLICT":
        conflicts.append("CURRENT_ADDRESS_CONFLICT")
    if name.get("effective_branch_warning_codes") or name.get("branch_warning_codes") or signals.get("name") == "CONFLICT":
        conflicts.append("CURRENT_BRANCH_OR_NAME_WARNING")
    group_resolved = len(source_group) > 1 and all(current_human[k] == "NON_MATCH" for k in source_group if k != key(er))
    group_conflict_only = v3.get("multiple_candidates") is True and group_resolved and "IDENTITY_METADATA_CONFLICT" not in v3.get("review_reason_codes", [])
    if signals.get("candidate") == "CONFLICT" and not group_conflict_only:
        conflicts.append("CANDIDATE_IDENTITY_CONFLICT")
    metadata_conflicts = json.loads(google.get("conflicting_values_json") or "{}")
    if google.get("has_metadata_conflict") or google.get("conflict_fields") or metadata_conflicts or evidence.get("google", {}).get("conflicting_values"):
        conflicts.append("METADATA_CONFLICT")
    if er["classification"] == "NON_MATCH_CANDIDATE":
        conflicts.append("ER_NON_MATCH_PROPOSAL")
    for signal in policy["required_v3_signals"]:
        if signals.get(signal) != "SUPPORT":
            gaps.append(signal.upper() + "_SUPPORT_MISSING")
    if address.get("strong_agreement") is not True:
        gaps.append("STRONG_ADDRESS_AGREEMENT_MISSING")
    if address.get("specific_agreement") is not True:
        gaps.append("SPECIFIC_ADDRESS_AGREEMENT_MISSING")
    if address.get("uncertain_difference"):
        gaps.append("UNCERTAIN_ADDRESS_DIFFERENCE")
    if address.get("reference_only"):
        gaps.append("REFERENCE_ONLY_ADDRESS")
    if google.get("metadata_status") not in {"AVAILABLE", "INCOMPLETE_METADATA"} or not er.get("overture_name") or not er.get("google_name") or not coordinate_valid(google) or not coordinate_valid(evidence.get("overture", {})):
        gaps.append("ESSENTIAL_METADATA_MISSING_OR_INVALID")
    distance = er.get("distance_m")
    if not isinstance(distance, (int, float)) or not math.isfinite(distance) or distance < 0:
        gaps.append("VALID_DISTANCE_MISSING")
    observed = dict(signals=signals, address=address, name=name, distance_m=distance,
        coordinates={"overture": {f:evidence.get("overture", {}).get(f) for f in ("latitude", "longitude")},
                     "google": {f:google.get(f) for f in ("latitude", "longitude")}},
        google_raw_metadata_ref=evidence.get("google", {}).get("metadata_ref"),
        google_metadata_status=google.get("metadata_status"), metadata_conflicts=metadata_conflicts,
        google_conflict_fields=google.get("conflict_fields"), source_flags=source_flags,
        multiple_candidate_group_resolved_by_current_human_non_match=group_resolved)
    return sorted(set(conflicts)), sorted(set(gaps)), observed


# The same canonical policy owns strict acceptance; no secondary matching thresholds.
EVIDENCE_REQUIREMENTS = dict(policy_version=VERSION, required_v3_signals=["name", "address", "location"])


def final_decision(row):
    if row['effective_status'] in {'AUTO_ACCEPTED','HUMAN_ACCEPTED'}:
        return 'MATCH'
    if row['effective_status'] == 'REJECTED':
        return 'NON_MATCH'
    if row['human_label'] or row['effective_status'] == 'CONFLICT':
        return 'REVIEW_REQUIRED'
    if row['er_classification'] == 'NON_MATCH_CANDIDATE':
        return 'NON_MATCH'
    if row['er_classification'] == 'INSUFFICIENT_DATA' or row['support_gaps']:
        return 'INSUFFICIENT_DATA'
    return 'REVIEW_REQUIRED'
