from __future__ import annotations

import logging
from importlib import import_module

from prettytable import PrettyTable

from passivbot_utils import calc_pnl
from utils import symbol_to_coin


def _get_pbr():
    return import_module("passivbot_rust")


async def log_position_changes(self, positions_old, positions_new, rd=6):
    """Log position transitions for debugging when differences are detected."""
    pbr = _get_pbr()
    psold = {(x["symbol"], x["position_side"]): {k: x[k] for k in ["size", "price"]} for x in positions_old}
    psnew = {(x["symbol"], x["position_side"]): {k: x[k] for k in ["size", "price"]} for x in positions_new}

    if psold == psnew:
        return

    for k in psnew:
        if k not in psold:
            psold[k] = {"size": 0.0, "price": 0.0}
    for k in psold:
        if k not in psnew:
            psnew[k] = {"size": 0.0, "price": 0.0}

    changed = [k for k in psnew if psold[k] != psnew[k]]
    if not changed:
        return

    total_we_by_pside = {"long": 0.0, "short": 0.0}
    balance_raw = self.get_raw_balance()
    for pos in positions_new:
        sym = pos["symbol"]
        ps = pos["position_side"]
        sz = pos.get("size", 0.0)
        px = pos.get("price", 0.0)
        if sz != 0 and balance_raw > 0 and sym in self.c_mults:
            total_we_by_pside[ps] += pbr.qty_to_cost(sz, px, self.c_mults[sym]) / balance_raw

    table = PrettyTable()
    table.border = False
    table.header = False
    table.padding_width = 0

    for symbol, pside in changed:
        old = psold[(symbol, pside)]
        new = psnew[(symbol, pside)]
        if old["size"] == 0.0 and new["size"] != 0.0:
            action = "    new"
        elif new["size"] == 0.0:
            action = " closed"
        elif new["size"] > old["size"]:
            action = "  added"
        elif new["size"] < old["size"]:
            action = "reduced"
        else:
            action = "unknown"

        wallet_exposure = (
            pbr.qty_to_cost(new["size"], new["price"], self.c_mults[symbol]) / balance_raw
            if new["size"] != 0 and balance_raw > 0
            else 0.0
        )
        wel = float(self.bp(pside, "wallet_exposure_limit", symbol))
        allowance_pct = float(self.bp(pside, "risk_we_excess_allowance_pct", symbol))
        effective_wel = wel * (1.0 + max(0.0, allowance_pct))
        wel_ratio = wallet_exposure / wel if wel > 0.0 else 0.0
        wele_ratio = wallet_exposure / effective_wel if effective_wel > 0.0 else 0.0

        last_price = await self.cm.get_current_close(symbol, max_age_ms=60_000)
        try:
            pprice_diff = (
                pbr.calc_pprice_diff_int(self.pside_int_map[pside], new["price"], last_price)
                if last_price
                else 0.0
            )
        except Exception:
            pprice_diff = 0.0

        try:
            upnl = (
                calc_pnl(pside, new["price"], last_price, new["size"], self.inverse, self.c_mults[symbol])
                if last_price
                else 0.0
            )
        except Exception:
            upnl = 0.0

        coin = symbol_to_coin(symbol, verbose=False) or symbol
        wel_pct = round(wel_ratio * 100)
        wele_pct = round(wele_ratio * 100)
        twel = float(self.bot_value(pside, "total_wallet_exposure_limit") or 0.0)
        twel_pct = round(total_we_by_pside[pside] / twel * 100) if twel > 0.0 else 0
        wel_str = f"| {wel_pct:3d}% WEL, {wele_pct:3d}% WELe, {twel_pct:3d}% TWEL |"
        table.add_row(
            [
                action + " ", coin + " ", pside + " ", pbr.round_dynamic(old["size"], rd), " @ ",
                pbr.round_dynamic(old["price"], rd), " -> ", pbr.round_dynamic(new["size"], rd), " @ ",
                pbr.round_dynamic(new["price"], rd), " WE: ", pbr.round_dynamic(wallet_exposure, 3), " ",
                wel_str, " PA dist: ", round(pprice_diff, 4), " upnl: ", pbr.round_dynamic(upnl, 3),
            ]
        )

    for line in table.get_string().splitlines():
        logging.info("[pos] %s", line)
