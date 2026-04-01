from __future__ import annotations

import math

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import managed_connection

# --- Thresholds ---
MIN_SIGNALS = 20
MIN_AVG_RETURN_5S = 0.0
MIN_FTR_5S = 0.55

STRONG_RETURN = 0.0
STRONG_FTR = 0.58
STRONG_SIGNALS = 30

WEAK_FTR_LOW = 0.52
WEAK_FTR_HIGH = 0.55


def _edge_score(avg_return_5s: float, signal_count: int, ftr_5s: float) -> float:
    """Composite score balancing magnitude, consistency, and sample size."""
    return avg_return_5s * math.log(max(signal_count, 1)) * ftr_5s


def _classify(
    avg_return_5s: float,
    ftr_5s: float,
    signal_count: int,
) -> str:
    if avg_return_5s > STRONG_RETURN and ftr_5s >= STRONG_FTR and signal_count >= STRONG_SIGNALS:
        return "STRONG"
    if avg_return_5s > 0 and WEAK_FTR_LOW <= ftr_5s < WEAK_FTR_HIGH:
        return "WEAK"
    if avg_return_5s <= 0:
        return "NO_EDGE"
    # positive return, ftr between thresholds but not yet STRONG
    return "WEAK"


def rank_wallets(
    settings: Settings | None = None,
    *,
    min_signals: int = MIN_SIGNALS,
    min_avg_return_5s: float = MIN_AVG_RETURN_5S,
    min_ftr_5s: float = MIN_FTR_5S,
    top_n: int = 50,
) -> list[dict]:
    settings = settings or get_settings()

    with managed_connection(settings) as conn:
        rows = conn.execute(
            """
            SELECT
                wps.wallet,
                COUNT(*)                                            AS signal_count,
                AVG(wpo.return_5s)                                  AS avg_return_5s,
                AVG(wpo.return_10s)                                 AS avg_return_10s,
                AVG(wpo.return_3s)                                  AS avg_return_3s,
                SUM(wpo.follow_through_5s)  * 1.0 / COUNT(*)       AS ftr_5s,
                SUM(wpo.follow_through_10s) * 1.0 / COUNT(*)       AS ftr_10s,
                SUM(wpo.follow_through_3s)  * 1.0 / COUNT(*)       AS ftr_3s,
                AVG(wpo.max_favorable_return)                       AS avg_mfe,
                AVG(wpo.max_adverse_return)                         AS avg_mae,
                AVG(wps.signal_score)                               AS avg_signal_score,
                AVG(wps.aligned_wallet_count)                       AS avg_aligned_wallets
            FROM wallet_paper_signals  wps
            JOIN wallet_paper_outcomes wpo ON wpo.signal_id = wps.id
            GROUP BY wps.wallet
            HAVING
                signal_count    >= ?
                AND avg_return_5s >= ?
                AND ftr_5s        >= ?
            ORDER BY avg_return_5s DESC
            LIMIT ?
            """,
            (min_signals, min_avg_return_5s, min_ftr_5s, top_n),
        ).fetchall()

    results = []
    for row in rows:
        d = dict(row)
        d["edge_score"] = _edge_score(d["avg_return_5s"], d["signal_count"], d["ftr_5s"])
        d["tier"] = _classify(d["avg_return_5s"], d["ftr_5s"], d["signal_count"])
        results.append(d)

    results.sort(key=lambda r: r["edge_score"], reverse=True)
    return results


def _count_all_wallets(settings: Settings | None = None) -> dict[str, int]:
    """Count wallets in each tier including those filtered out."""
    settings = settings or get_settings()
    with managed_connection(settings) as conn:
        rows = conn.execute(
            """
            SELECT
                wps.wallet,
                COUNT(*)                                        AS signal_count,
                AVG(wpo.return_5s)                              AS avg_return_5s,
                SUM(wpo.follow_through_5s) * 1.0 / COUNT(*)    AS ftr_5s
            FROM wallet_paper_signals  wps
            JOIN wallet_paper_outcomes wpo ON wpo.signal_id = wps.id
            GROUP BY wps.wallet
            HAVING signal_count >= 1
            """,
        ).fetchall()

    counts = {"total": len(rows), "no_data": 0}
    for row in rows:
        tier = _classify(row["avg_return_5s"], row["ftr_5s"], row["signal_count"])
        counts[tier] = counts.get(tier, 0) + 1
    return counts


def print_leaderboard(settings: Settings | None = None) -> None:
    settings = settings or get_settings()
    wallets = rank_wallets(settings)
    counts = _count_all_wallets(settings)

    strong = [w for w in wallets if w["tier"] == "STRONG"]
    weak   = [w for w in wallets if w["tier"] == "WEAK"]

    # ── Summary banner ────────────────────────────────────────────────────────
    print()
    print("=" * 75)
    print("  WALLET EDGE LEADERBOARD")
    print("=" * 75)
    print(
        f"  Total wallets with paper signals : {counts.get('total', 0)}"
    )
    print(
        f"  STRONG  (ftr>=0.58, ret>0, n>=30): {counts.get('STRONG', 0)}"
    )
    print(
        f"  WEAK    (ret>0, ftr 0.52-0.58)   : {counts.get('WEAK', 0)}"
    )
    print(
        f"  NO_EDGE (ret<=0)                 : {counts.get('NO_EDGE', 0)}"
    )
    print()

    if not wallets:
        print("  ⚠  No wallets passed the minimum filters.")
        print(f"     (min_signals={MIN_SIGNALS}, min_ftr_5s={MIN_FTR_5S}, min_ret_5s={MIN_AVG_RETURN_5S})")
        print()
        _print_verdict(strong, weak)
        return

    # ── Table header ─────────────────────────────────────────────────────────
    header = (
        f"{'WALLET':<44} {'SIG':>5} {'RET_5s':>8} {'RET_10s':>8} "
        f"{'FTR_5s':>7} {'FTR_10s':>7} {'MFE':>7} {'MAE':>7} "
        f"{'SCORE':>7} {'TIER':<9}"
    )
    print(header)
    print("-" * 115)

    for w in wallets:
        tier_label = {
            "STRONG": "[STRONG]",
            "WEAK":   "[WEAK]  ",
            "NO_EDGE": "[NO_EDGE]",
        }.get(w["tier"], w["tier"])

        ret_5s  = w["avg_return_5s"]  or 0.0
        ret_10s = w["avg_return_10s"] or 0.0
        ftr_5s  = w["ftr_5s"]         or 0.0
        ftr_10s = w["ftr_10s"]        or 0.0
        mfe     = w["avg_mfe"]         or 0.0
        mae     = w["avg_mae"]         or 0.0

        print(
            f"{w['wallet']:<44} "
            f"{w['signal_count']:>5} "
            f"{ret_5s:>+8.4f} "
            f"{ret_10s:>+8.4f} "
            f"{ftr_5s:>7.3f} "
            f"{ftr_10s:>7.3f} "
            f"{mfe:>+7.4f} "
            f"{mae:>+7.4f} "
            f"{w['edge_score']:>7.4f} "
            f"{tier_label}"
        )

    print()
    _print_verdict(strong, weak)


def _print_verdict(strong: list[dict], weak: list[dict]) -> None:
    print("=" * 75)
    if strong:
        print(f"  GO — {len(strong)} wallet(s) show STRONG forward edge. Trade these.")
        for w in strong:
            print(f"       {w['wallet']}  edge_score={w['edge_score']:.4f}")
    elif weak:
        print(f"  CAUTION — {len(weak)} wallet(s) show WEAK edge. Needs more data or regime filter.")
    else:
        print("  NO-GO — No wallets pass the edge threshold. Do not trade wallet signals yet.")
    print("=" * 75)
    print()


if __name__ == "__main__":
    print_leaderboard()
