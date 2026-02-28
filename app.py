"""
HAGETAKA SCOPE - 日次候補抽出（GitHub Actions用）

- 目的：市場データの“状態”を可視化し、候補を少数に絞る
- 本ツールは補助ツールであり、銘柄推奨・売買助言ではありません

出力：data/ratios.json
- data: 候補（フィルタ済み・並び替え済み）
- all_data: 参考（時価総額フィルタ済み）
"""

import json
import os
from datetime import datetime
from pathlib import Path
import time

import numpy as np
import pandas as pd
import pytz
import yfinance as yf


JST = pytz.timezone("Asia/Tokyo")

MARKET_CAP_MIN = 300
MARKET_CAP_MAX = 2000

FLOW_SCORE_HIGH = 70
FLOW_SCORE_MEDIUM = 40

# 監視銘柄（例）
MIDCAP_TICKERS = [
    # ...（既存のまま）
]


def calculate_volume_profile(df: pd.DataFrame, bins: int = 24) -> pd.DataFrame:
    """価格帯別売買高（簡易）を計算（6か月足で使用）"""
    if df is None or df.empty:
        return pd.DataFrame()

    price_min = float(df["Low"].min())
    price_max = float(df["High"].max())
    if not np.isfinite(price_min) or not np.isfinite(price_max) or price_max <= price_min:
        return pd.DataFrame()

    price_bins = np.linspace(price_min, price_max, int(bins) + 1)

    volume_profile = []
    for i in range(len(price_bins) - 1):
        bin_low = float(price_bins[i])
        bin_high = float(price_bins[i + 1])
        bin_center = (bin_low + bin_high) / 2.0

        total_volume = 0.0
        for _, row in df.iterrows():
            low = float(row["Low"])
            high = float(row["High"])
            vol = float(row["Volume"])
            if low <= bin_high and high >= bin_low:
                overlap_low = max(low, bin_low)
                overlap_high = min(high, bin_high)
                if high > low:
                    ratio = (overlap_high - overlap_low) / (high - low)
                else:
                    ratio = 1.0
                total_volume += vol * ratio

        volume_profile.append({"price": bin_center, "price_low": bin_low, "price_high": bin_high, "volume": total_volume})

    return pd.DataFrame(volume_profile)


def calculate_volume_profile_with_bins(df: pd.DataFrame, price_bins: np.ndarray) -> pd.DataFrame:
    """同じprice_binsを使って価格帯別売買高を計算（差分用）"""
    if df is None or df.empty or price_bins is None or len(price_bins) < 2:
        return pd.DataFrame()

    volume_profile = []
    for i in range(len(price_bins) - 1):
        bin_low = float(price_bins[i])
        bin_high = float(price_bins[i + 1])
        bin_center = (bin_low + bin_high) / 2.0

        total_volume = 0.0
        for _, row in df.iterrows():
            low = float(row["Low"])
            high = float(row["High"])
            vol = float(row["Volume"])
            if low <= bin_high and high >= bin_low:
                overlap_low = max(low, bin_low)
                overlap_high = min(high, bin_high)
                if high > low:
                    ratio = (overlap_high - overlap_low) / (high - low)
                else:
                    ratio = 1.0
                total_volume += vol * ratio

        volume_profile.append({"price": bin_center, "price_low": bin_low, "price_high": bin_high, "volume": total_volume})

    return pd.DataFrame(volume_profile)


def compute_support_from_recent_growth(
    df: pd.DataFrame,
    bins: int = 24,
    recent_ratio: float = 0.33,
    low_band_ratio: float = 0.35,
):
    """下値ラインを「直近で価格帯別売買高が伸びた帯 × 安値付近」から選ぶ。

    返り値: (support_price, support_upper_price)
    """
    if df is None or df.empty or len(df) < 40:
        return None, None

    price_min = float(df["Low"].min())
    price_max = float(df["High"].max())
    if not np.isfinite(price_min) or not np.isfinite(price_max) or price_max <= price_min:
        return None, None

    price_bins = np.linspace(price_min, price_max, int(bins) + 1)

    n = len(df)
    recent_len = max(20, int(n * float(recent_ratio)))
    if n < recent_len * 2:
        return None, None

    recent_df = df.tail(recent_len)
    prev_df = df.iloc[-recent_len * 2 : -recent_len]

    vp_recent = calculate_volume_profile_with_bins(recent_df, price_bins)
    vp_prev = calculate_volume_profile_with_bins(prev_df, price_bins)
    if vp_recent.empty or vp_prev.empty:
        return None, None

    vp = vp_recent.copy()
    vp["prev_volume"] = vp_prev["volume"].values
    vp["growth"] = vp["volume"] - vp["prev_volume"]

    low_limit = price_min + (price_max - price_min) * float(low_band_ratio)
    cand = vp[vp["price_high"] <= low_limit].copy()
    if cand.empty:
        return None, None

    cand = cand.sort_values("growth", ascending=False)
    best = cand.iloc[0]
    if not np.isfinite(float(best.get("growth", 0.0))) or float(best.get("growth", 0.0)) <= 0:
        return None, None

    return float(best["price_low"]), float(best["price_high"])


def compute_support_zone_from_profile(vp: pd.DataFrame, threshold_ratio: float = 0.60):
    """高出来高ゾーン（POC周辺）を抽出し、その下限を下値ラインとして返す。

    返り値: (support_price, zone_upper_price)
    """
    if vp is None or vp.empty:
        return None, None
    if "volume" not in vp.columns:
        return None, None

    max_vol = float(vp["volume"].max())
    if max_vol <= 0:
        return None, None

    vp_reset = vp.reset_index(drop=True)
    try:
        poc_pos = int(vp_reset["volume"].idxmax())
    except Exception:
        poc_pos = 0
    thr = max_vol * float(threshold_ratio)

    left = poc_pos
    right = poc_pos
    while left - 1 >= 0 and float(vp_reset.loc[left - 1, "volume"]) >= thr:
        left -= 1
    while right + 1 < len(vp_reset) and float(vp_reset.loc[right + 1, "volume"]) >= thr:
        right += 1

    support = float(vp_reset.loc[left, "price_low"])
    upper = float(vp_reset.loc[right, "price_high"])
    return support, upper


def support_position_tag(latest_price: float, support_price: float | None) -> tuple[str | None, float | None]:
    """下値ラインからの位置（％）を計算し、中立的なタグ名を返す。
    - 下側ゾーン: +3%以内（下値ラインに近い）
    - 上側ゾーン: +25%以上（下値ラインから大きく離れている）
    """
    if support_price is None or support_price <= 0:
        return None, None
    gap_pct = (latest_price / support_price - 1.0) * 100.0

    if gap_pct <= 3.0:
        return "下側ゾーン", float(gap_pct)
    if gap_pct >= 25.0:
        return "上側ゾーン", float(gap_pct)
    return None, float(gap_pct)


# 以下、既存ロジック（flow_score計算、reorg_score計算、event_score計算、determine_level等）は元のまま
# ---------------------------------------------------------
# ※ここはあなたの元ファイルの関数群が続いている前提です
# ---------------------------------------------------------


def fetch_volume_data(tickers: list[str]):
    results = {}
    qualified = {}

    for ticker in tickers:
        try:
            df = yf.download(ticker, period="6mo", interval="1d", progress=False)
            if df is None or df.empty:
                continue
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = df.dropna()
            if len(df) < 30:
                continue

            latest_price = float(df["Close"].iloc[-1])
            latest_volume = float(df["Volume"].iloc[-1])
            avg_volume = float(df["Volume"].tail(60).mean()) if len(df) >= 60 else float(df["Volume"].mean())
            vol_ratio = round(latest_volume / avg_volume, 2) if avg_volume > 0 else 0

            # （中略）flow_score / flow_details / watch_flag / streak / reorg_score / event_score などは既存のまま

            # --- 下値ライン（6か月） ---
            support_price = None
            support_upper = None
            support_gap_pct = None
            support_tag = None
            try:
                df_6m = df.tail(126).copy()
                vp6 = calculate_volume_profile(df_6m, bins=24)

                support_price, support_upper = compute_support_from_recent_growth(
                    df_6m,
                    bins=24,
                    recent_ratio=0.33,
                    low_band_ratio=0.35,
                )

                if support_price is None:
                    support_price, support_upper = compute_support_zone_from_profile(vp6, threshold_ratio=0.60)

                support_tag, support_gap_pct = support_position_tag(latest_price, support_price)
            except Exception:
                pass

            # --- 企業情報（株数比用） ---
            market_cap_oku = 0.0
            pbr = None
            shares_outstanding = None
            name = ticker
            stock = None
            try:
                stock = yf.Ticker(ticker)
                info = stock.info or {}
                name = info.get("shortName") or info.get("longName") or name
                mcap = info.get("marketCap")
                if mcap:
                    market_cap_oku = float(mcap) / 1e8
                pbr = info.get("priceToBook")
                shares_outstanding = info.get("sharesOutstanding")
            except Exception:
                pass

            in_range = (market_cap_oku >= MARKET_CAP_MIN) and (market_cap_oku <= MARKET_CAP_MAX)

            volume_of_shares_pct = None
            if shares_outstanding and shares_outstanding > 0:
                try:
                    volume_of_shares_pct = (float(latest_volume) / float(shares_outstanding)) * 100.0
                except Exception:
                    volume_of_shares_pct = None

            # tags（既存 + 下側/上側ゾーン + 要監視）
            tags = []
            if support_tag:
                tags.append(support_tag)

            # ここは元ファイルのwatch判定結果を使ってください
            # watch_flag = ...
            # flow_details = ...

            # （例として）旧変数がある前提で追加
            try:
                if watch_flag:
                    tags.append("要監視")
            except Exception:
                pass

            try:
                if flow_details.get("vol_anomaly", 0) >= 50:
                    tags.append("出来高変化")
            except Exception:
                pass

            result = {
                "name": name,
                "price": round(latest_price, 1),
                "volume": int(latest_volume),
                "avg_volume": int(avg_volume),
                "vol_ratio": vol_ratio,
                "shares_outstanding": int(shares_outstanding) if shares_outstanding else None,
                "volume_of_shares_pct": round(float(volume_of_shares_pct), 3) if volume_of_shares_pct is not None else None,
                "market_cap_oku": int(market_cap_oku) if market_cap_oku else 0,
                "pbr": round(float(pbr), 2) if pbr else None,
                "in_cap_range": in_range,
                # ここ以下は元のキー（level / ma_score / flow_score / flow_details / display_state 等）を維持
                "support_price": round(float(support_price), 1) if support_price else None,
                "support_upper": round(float(support_upper), 1) if support_upper else None,
                "support_gap_pct": round(float(support_gap_pct), 1) if support_gap_pct is not None else None,
                "tags": tags,
            }

            results[ticker] = result

            # ここは元の条件（in_range & flow_score閾値など）を維持
            try:
                if in_range and (result.get("flow_score", 0) >= FLOW_SCORE_MEDIUM):
                    qualified[ticker] = result
            except Exception:
                pass

        except Exception:
            continue

        time.sleep(0.25)

    return results, qualified


def main():
    now_jst = datetime.now(JST)
    updated_at = now_jst.strftime("%Y-%m-%d %H:%M:%S")

    results, qualified = fetch_volume_data(MIDCAP_TICKERS)

    filtered = {k: v for k, v in results.items() if v.get("in_cap_range")}

    sorted_qualified = dict(
        sorted(
            qualified.items(),
            key=lambda x: (int(x[1].get("level", 0)), float(x[1].get("ma_score", 0)), float(x[1].get("flow_score", 0))),
            reverse=True,
        )
    )
    sorted_filtered = dict(
        sorted(
            filtered.items(),
            key=lambda x: (int(x[1].get("level", 0)), float(x[1].get("ma_score", 0)), float(x[1].get("flow_score", 0))),
            reverse=True,
        )
    )

    level_counts = {}
    for r in sorted_qualified.values():
        lv = int(r.get("level", 0))
        level_counts[lv] = level_counts.get(lv, 0) + 1

    output = {
        "updated_at": updated_at,
        "date": now_jst.strftime("%Y-%m-%d"),
        "market_cap_range": f"{MARKET_CAP_MIN}億〜{MARKET_CAP_MAX}億円",
        "total_count": len(sorted_qualified),
        "all_count": len(results),
        "filtered_count": len(filtered),
        "level_counts": level_counts,
        "data": sorted_qualified,
        "all_data": sorted_filtered,
        "disclaimer": "本ツールは市場データの可視化を目的とした補助ツールです。銘柄推奨・売買助言ではありません。",
    }

    os.makedirs("data", exist_ok=True)
    Path("data/ratios.json").write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print("💾 保存完了: data/ratios.json")
    print(f"🎯 候補: {len(sorted_qualified)} 件 / フィルタ通過: {len(filtered)} 件")


if __name__ == "__main__":
    main()
