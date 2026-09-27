from dataclasses import dataclass


@dataclass(frozen=True)
class NetworkIdentity:
    id: str
    proxy_url: str | None = None
