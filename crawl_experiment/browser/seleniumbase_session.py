from __future__ import annotations

from collections.abc import Callable
from typing import Any, Self

from crawl_experiment.core.errors import BrowserSessionError
from crawl_experiment.network.network_identity import NetworkIdentity


class SeleniumBaseSession:
    """Owns one SeleniumBase UC driver and no source-specific behavior."""

    def __init__(
        self,
        identity: NetworkIdentity,
        driver_builder: Callable[..., Any] | None = None,
        *,
        headless: bool = True,
    ):
        self.identity = identity
        self._builder = driver_builder
        self.headless = headless
        self.driver: Any | None = None

    def start(self) -> Any:
        if self.driver is not None:
            return self.driver
        try:
            if self._builder is None:
                from seleniumbase import Driver

                self._builder = Driver
            options = {"uc": True, "headless": self.headless}
            if self.identity.proxy_url:
                options["proxy"] = self.identity.proxy_url
            self.driver = self._builder(**options)
            return self.driver
        except Exception as exc:
            raise BrowserSessionError(f"could not create SeleniumBase session: {exc}") from exc

    def is_alive(self) -> bool:
        if self.driver is None:
            return False
        try:
            _ = self.driver.current_url
            return True
        except Exception:  # noqa: BLE001
            return False

    def require_alive(self) -> Any:
        if not self.is_alive():
            raise BrowserSessionError("WebDriver session is no longer alive")
        return self.driver

    def close(self) -> None:
        driver, self.driver = self.driver, None
        if driver is not None:
            processes = self._driver_processes(driver)
            try:
                driver.quit()
            except Exception:  # noqa: BLE001,S110
                pass
            finally:
                for process in processes:
                    self._suppress_invalid_windows_handle(process)

    @staticmethod
    def _driver_processes(driver: Any) -> list[Any]:
        service = getattr(driver, "service", None)
        candidates = (
            getattr(driver, "_service_process", None),
            getattr(driver, "browser_process", None),
            getattr(service, "process", None),
        )
        return list({id(process): process for process in candidates if process}.values())

    @staticmethod
    def _suppress_invalid_windows_handle(process: Any) -> None:
        try:
            process.poll()
        except OSError as exc:
            if getattr(exc, "winerror", None) == 6:
                process._child_created = False

    def restart(self) -> Any:
        self.close()
        return self.start()

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
