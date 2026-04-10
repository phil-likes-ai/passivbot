from __future__ import annotations

import logging
import os
import traceback
from pathlib import Path

from fill_events_manager import FillEventsManager, _build_fetcher_for_bot, _extract_symbol_pool


async def init_pnls(self):
    """Initialize FillEventsManager for PnL tracking."""
    if self._pnls_initialized:
        return

    try:
        logging.info("[fills] initializing FillEventsManager")

        symbol_pool = _extract_symbol_pool(self.config, None)
        fetcher = _build_fetcher_for_bot(self, symbol_pool)
        cache_path = Path(f"caches/fill_events/{self.exchange}/{self.user}")

        self._pnls_manager = FillEventsManager(
            exchange=self.exchange,
            user=self.user,
            fetcher=fetcher,
            cache_path=cache_path,
        )

        await self._pnls_manager.ensure_loaded()

        doctor_mode = str(os.getenv("PASSIVBOT_FILL_EVENTS_DOCTOR", "")).strip().lower()
        if self.exchange == "bybit":
            if doctor_mode not in ("0", "false", "off", "disable", "disabled"):
                auto_repair = doctor_mode not in ("check", "scan", "detect")
                report = await self._pnls_manager.run_doctor(auto_repair=auto_repair)
                logging.info(
                    "[fills-doctor] startup report anomalies=%s repaired=%s mode=%s",
                    report.get("anomaly_events", 0),
                    report.get("repaired", False),
                    doctor_mode or ("repair" if auto_repair else "check"),
                )
        elif doctor_mode:
            auto_repair = doctor_mode in ("1", "true", "yes", "repair", "fix", "auto")
            report = await self._pnls_manager.run_doctor(auto_repair=auto_repair)
            logging.info(
                "[fills-doctor] startup report anomalies=%s repaired=%s mode=%s",
                report.get("anomaly_events", 0),
                report.get("repaired", False),
                doctor_mode,
            )

        cached_count = len(self._pnls_manager._events)
        logging.info("[fills] initialized: %d cached events loaded", cached_count)
        self._pnls_initialized = True

    except Exception as e:
        logging.error("Failed to initialize FillEventsManager: %s", e)
        traceback.print_exc()
        raise
