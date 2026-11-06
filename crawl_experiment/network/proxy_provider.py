from typing import Protocol

from .network_identity import NetworkIdentity


class ProxyProvider(Protocol):
    def identity_for(self, session_id: str) -> NetworkIdentity: ...


class DirectProxyProvider:
    def identity_for(self, session_id: str) -> NetworkIdentity:
        return NetworkIdentity(id=session_id)
