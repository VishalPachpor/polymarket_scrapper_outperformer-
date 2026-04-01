from __future__ import annotations

from dataclasses import dataclass

from tip_v1.config import Settings, get_settings
from tip_v1.wallet_engine.wallet_scoring import WalletBaseScore, load_wallet_base_scores


@dataclass(frozen=True)
class WalletRegistryConfig:
    regime: str = "global"
    top_n: int = 100
    refresh_interval_seconds: int = 300


class WalletRegistry:
    def __init__(
        self,
        *,
        settings: Settings | None = None,
        config: WalletRegistryConfig | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.config = config or WalletRegistryConfig()
        self._wallets: dict[str, WalletBaseScore] = {}
        self._last_refresh_ts: int | None = None

    def refresh(self, *, now_ts: int) -> dict[str, WalletBaseScore]:
        if (
            self._last_refresh_ts is not None
            and now_ts - self._last_refresh_ts < self.config.refresh_interval_seconds
        ):
            return self._wallets
        self._wallets = load_wallet_base_scores(
            settings=self.settings,
            regime=self.config.regime,
            limit=self.config.top_n,
        )
        self._last_refresh_ts = now_ts
        return self._wallets

    def get(self, wallet: str) -> WalletBaseScore | None:
        return self._wallets.get(wallet.lower())

    def wallets(self) -> dict[str, WalletBaseScore]:
        return dict(self._wallets)
