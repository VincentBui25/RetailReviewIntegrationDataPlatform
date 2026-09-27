from collections.abc import Callable
from typing import Any

from crawl_experiment.network.proxy_provider import DirectProxyProvider, ProxyProvider

from .seleniumbase_session import SeleniumBaseSession


class BrowserFactory:
    def __init__(
        self,
        proxy_provider: ProxyProvider | None = None,
        driver_builder: Callable[..., Any] | None = None,
        *,
        headless: bool = True,
    ):
        self.proxy_provider = proxy_provider or DirectProxyProvider()
        self.driver_builder = driver_builder
        self.headless = headless

    def create(self, session_id: str) -> SeleniumBaseSession:
        identity = self.proxy_provider.identity_for(session_id)
        return SeleniumBaseSession(identity, self.driver_builder, headless=self.headless)
