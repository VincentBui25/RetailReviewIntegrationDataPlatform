from dataclasses import dataclass
from enum import StrEnum

from . import selectors


class HealthState(StrEnum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    LIMITED = "limited"
    RATE_LIMITED = "rate_limited"
    CHALLENGE = "challenge"


@dataclass(frozen=True)
class PageEvidence:
    has_place_tabs: bool = False
    has_review_ui: bool = False
    has_place_shell: bool = False
    has_sign_in_prompt: bool = False

    @property
    def has_place(self) -> bool:
        return self.has_place_tabs or self.has_review_ui or self.has_place_shell


def limited_signals(body_text: str, evidence: PageEvidence) -> tuple[str, ...]:
    text = (body_text or "").casefold()
    signals = [
        f"text:{phrase}"
        for phrase in selectors.LIMITED_VIEW_TEXT
        if phrase.casefold() in text
    ]
    prompt_text = [
        phrase
        for phrase in selectors.LIMITED_PROMPT_TEXT
        if phrase.casefold() in text
    ]
    if prompt_text and evidence.has_sign_in_prompt:
        signals.extend(f"prompt:{phrase}" for phrase in prompt_text)
        signals.append("ui:sign_in_prompt")
    return tuple(signals)


def classify_page(
    url: str,
    body_text: str,
    *,
    has_place: bool | None = None,
    evidence: PageEvidence | None = None,
) -> HealthState:
    url, text = (url or "").lower(), (body_text or "").lower()
    if "/sorry/" in url:
        return HealthState.RATE_LIMITED
    if "captcha" in url or "recaptcha" in url or any(x in text for x in selectors.CHALLENGE_TEXT):
        return HealthState.CHALLENGE
    if evidence is None:
        evidence = PageEvidence(has_place_shell=True if has_place is None else has_place)
    signals = limited_signals(text, evidence)
    if signals and not evidence.has_place:
        return HealthState.LIMITED
    return HealthState.HEALTHY if evidence.has_place else HealthState.DEGRADED
