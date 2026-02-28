"""
HAGETAKA SCOPE - M&A候補検知ツール
- ログイン機能（共通パスワード or 登録済みメールアドレス）
- FlowScore（FlowScoreの強度）によるM&A候補検知
- 利用者ごとのメール通知機能（Google Sheets永続化）
- チャート表示機能（ローソク足・出来高・価格帯別売買高）

※本ツールは市場データの可視化を目的とした補助ツールです。
※銘柄推奨・売買助言ではありません。
"""

import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, List, Optional
from pathlib import Path
import streamlit as st
from datetime import datetime, timedelta
import pytz
import base64
import re
import pandas as pd
import numpy as np

# チャート
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import yfinance as yf

# Google Sheets連携
from streamlit_gsheets import GSheetsConnection
import gspread
from google.oauth2.service_account import Credentials

# 暗号化
from cryptography.fernet import Fernet

# ==========================================
# 定数
# ==========================================
JST = pytz.timezone("Asia/Tokyo")
MARKET_CAP_MIN = 300
MARKET_CAP_MAX = 2000

# FlowScore閾値
FLOW_SCORE_HIGH = 70
FLOW_SCORE_MEDIUM = 40

# LEVELカラー（表示は数字のみ）
LEVEL_COLORS = {
    4: "#C41E3A",  # 赤
    3: "#FF9800",  # オレンジ
    2: "#FFC107",  # 黄
    1: "#5C6BC0",  # 青紫
    0: "#9E9E9E",  # グレー
}

# 共通ログインパスワード（初回用）
MASTER_PASSWORD = "88888"

# 免責文言
DISCLAIMER_TEXT = "本ツールは市場データの可視化を目的とした補助ツールです。銘柄推奨・売買助言ではありません。最終判断は利用者ご自身で行ってください。"

# ==========================================
# 日本語銘柄名辞書
# ==========================================
TICKER_NAMES_JP = {
    "3923.T": "ラクス",
    "4443.T": "Sansan",
    "4478.T": "フリー",
    "3994.T": "マネーフォワード",
    "4165.T": "プレイド",
    "4169.T": "ENECHANGE",
    "4449.T": "ギフティ",
    "4475.T": "HENNGE",
    "4431.T": "スマレジ",
    "4057.T": "インターファクトリー",
    "3697.T": "SHIFT",
    "4194.T": "ビジョナル",
    "4180.T": "Appier",
    "3655.T": "ブレインパッド",
    "4751.T": "サイバーエージェント",
    "3681.T": "ブイキューブ",
    "6035.T": "IRジャパン",
    "4384.T": "ラクスル",
    "9558.T": "ジャパニアス",
    "4441.T": "トビラシステムズ",
    "6315.T": "TOWA",
    "6323.T": "ローツェ",
    "6890.T": "フェローテック",
    "7735.T": "SCREEN",
    "6857.T": "アドバンテスト",
    "8035.T": "東京エレクトロン",
    "9984.T": "ソフトバンクG",
}

# ==========================================
# データ読み込み
# ==========================================
@st.cache_data(ttl=60)
def load_data() -> Dict:
    """JSONからデータを読み込み"""
    data_path = Path("data/ratios.json")
    if data_path.exists():
        with open(data_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


# ==========================================
# 暗号化・復号化
# ==========================================
def get_fernet() -> Fernet:
    """暗号化キーを取得してFernetインスタンスを返す"""
    encryption_key = st.secrets["encryption"]["key"]
    return Fernet(encryption_key.encode())


def encrypt_password(password: str) -> str:
    """パスワードを暗号化"""
    if not password:
        return ""
    fernet = get_fernet()
    encrypted = fernet.encrypt(password.encode())
    return encrypted.decode()


def decrypt_password(encrypted_password: str) -> str:
    """パスワードを復号化"""
    if not encrypted_password:
        return ""
    try:
        fernet = get_fernet()
        decrypted = fernet.decrypt(encrypted_password.encode())
        return decrypted.decode()
    except Exception:
        return ""


# ==========================================
# Google Sheets設定
# ==========================================
def get_sheets_connection():
    """Google Sheets接続を取得"""
    try:
        conn = st.connection("gsheets", type=GSheetsConnection)
        return conn
    except Exception:
        return None


def save_settings_to_sheet(email: str, app_password: str) -> bool:
    """通知設定をGoogle Sheetsに保存"""
    try:
        conn = get_sheets_connection()
        if conn is None:
            return False

        df = conn.read(worksheet="users", usecols=[0, 1], ttl=0)
        df = df.dropna(how="all")

        encrypted_pw = encrypt_password(app_password)

        if "email" not in df.columns:
            df.columns = ["email", "password"]

        if email in df["email"].values:
            df.loc[df["email"] == email, "password"] = encrypted_pw
        else:
            new_row = pd.DataFrame({"email": [email], "password": [encrypted_pw]})
            df = pd.concat([df, new_row], ignore_index=True)

        conn.update(worksheet="users", data=df)
        return True

    except Exception:
        return False


def is_registered_email(email: str) -> bool:
    """登録済みメールか確認"""
    try:
        conn = get_sheets_connection()
        if conn is None:
            return False
        df = conn.read(worksheet="users", usecols=[0], ttl=0)
        df = df.dropna(how="all")
        if df.empty:
            return False
        if "email" not in df.columns:
            df.columns = ["email"]
        return email in df["email"].values
    except Exception:
        return False


def get_user_password(email: str) -> str:
    """登録済みユーザーの復号パスワード取得"""
    try:
        conn = get_sheets_connection()
        if conn is None:
            return ""
        df = conn.read(worksheet="users", usecols=[0, 1], ttl=0)
        df = df.dropna(how="all")
        if df.empty:
            return ""
        if "email" not in df.columns:
            df.columns = ["email", "password"]
        row = df[df["email"] == email]
        if row.empty:
            return ""
        enc = row.iloc[0]["password"]
        return decrypt_password(enc)
    except Exception:
        return ""


# ==========================================
# メール送信（通知）
# ==========================================
def send_spike_alert(email: str, app_password: str, stocks: List[Dict], updated_at: str) -> bool:
    """出来高急動通知メールを送信"""
    try:
        smtp_server = "smtp.gmail.com"
        port = 587

        msg = MIMEMultipart()
        msg["From"] = email
        msg["To"] = email
        msg["Subject"] = f"【ハゲタカSCOPE】出来高急動の候補銘柄（{len(stocks)}件）"

        lines = []
        lines.append("ハゲタカSCOPE 検知結果（候補抽出）")
        lines.append(f"更新: {updated_at} JST")
        lines.append("")
        lines.append("※本メールは売買助言ではありません。市場データの状態変化の通知です。")
        lines.append("")

        for s in stocks[:30]:
            tk = s.get("ticker", "")
            name = s.get("name", "")
            level = s.get("level", "")
            flow = s.get("flow_score", "")
            state = s.get("display_state", s.get("state", ""))
            lines.append(f"- {tk} {name} / LEVEL {level} / Flow {flow} / 状態 {state}")

        body = "\n".join(lines)
        msg.attach(MIMEText(body, "plain"))

        server = smtplib.SMTP(smtp_server, port)
        server.starttls()
        server.login(email, app_password)
        server.send_message(msg)
        server.quit()
        return True
    except Exception:
        return False


# ==========================================
# チャート用データ取得
# ==========================================
@st.cache_data(ttl=60)
def fetch_chart_data(ticker: str, period: str) -> pd.DataFrame:
    """yfinanceでローソク足データ取得"""
    try:
        df = yf.download(ticker, period=period, interval="1d", progress=False)
        if df is None or df.empty:
            return pd.DataFrame()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df.dropna()
        return df
    except Exception:
        return pd.DataFrame()


def calculate_volume_profile(df: pd.DataFrame, bins: int = 20) -> pd.DataFrame:
    """価格帯別売買高を計算"""
    if df.empty:
        return pd.DataFrame()

    # 価格範囲を分割
    price_min = df["Low"].min()
    price_max = df["High"].max()
    price_bins = np.linspace(price_min, price_max, bins + 1)

    volume_profile = []

    for i in range(len(price_bins) - 1):
        bin_low = price_bins[i]
        bin_high = price_bins[i + 1]
        bin_center = (bin_low + bin_high) / 2

        # この価格帯に含まれる日の出来高を集計
        total_volume = 0
        for _, row in df.iterrows():
            if row["Low"] <= bin_high and row["High"] >= bin_low:
                # 出来高を価格帯に按分
                overlap_low = max(row["Low"], bin_low)
                overlap_high = min(row["High"], bin_high)
                if row["High"] > row["Low"]:
                    ratio = (overlap_high - overlap_low) / (row["High"] - row["Low"])
                else:
                    ratio = 1.0
                total_volume += row["Volume"] * ratio

        volume_profile.append({"price": bin_center, "price_low": bin_low, "price_high": bin_high, "volume": total_volume})

    return pd.DataFrame(volume_profile)


def calculate_volume_profile_with_bins(df: pd.DataFrame, price_bins: np.ndarray) -> pd.DataFrame:
    """同じprice_binsを使って価格帯別売買高を計算（差分用）"""
    if df.empty or price_bins is None or len(price_bins) < 2:
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
    bins: int,
    recent_ratio: float = 0.33,
    low_band_ratio: float = 0.35,
):
    """下値ラインを「直近で価格帯別売買高が伸びた帯 × 安値付近」から選ぶ。

    返り値: (support_price, support_upper_price, highlight_range_dict)
      - support_price: 選んだ帯の下限
      - support_upper_price: 選んだ帯の上限
      - highlight_range_dict: {'price_low':..., 'price_high':...}（VP強調用）
    """
    if df is None or df.empty or len(df) < 40:
        return None, None, None

    price_min = float(df["Low"].min())
    price_max = float(df["High"].max())
    if not np.isfinite(price_min) or not np.isfinite(price_max) or price_max <= price_min:
        return None, None, None

    price_bins = np.linspace(price_min, price_max, int(bins) + 1)

    n = len(df)
    recent_len = max(20, int(n * float(recent_ratio)))
    if n < recent_len * 2:
        return None, None, None

    recent_df = df.tail(recent_len)
    prev_df = df.iloc[-recent_len * 2 : -recent_len]

    vp_recent = calculate_volume_profile_with_bins(recent_df, price_bins)
    vp_prev = calculate_volume_profile_with_bins(prev_df, price_bins)
    if vp_recent.empty or vp_prev.empty:
        return None, None, None

    vp = vp_recent.copy()
    vp["prev_volume"] = vp_prev["volume"].values
    vp["growth"] = vp["volume"] - vp["prev_volume"]

    # 安値付近（下側）だけに絞る
    low_limit = price_min + (price_max - price_min) * float(low_band_ratio)
    cand = vp[vp["price_high"] <= low_limit].copy()
    if cand.empty:
        return None, None, None

    # 直近で伸びた帯を優先（growth最大）
    cand = cand.sort_values("growth", ascending=False)
    best = cand.iloc[0]
    if not np.isfinite(float(best.get("growth", 0.0))) or float(best.get("growth", 0.0)) <= 0:
        return None, None, None

    support_price = float(best["price_low"])
    support_upper = float(best["price_high"])
    return support_price, support_upper, {"price_low": support_price, "price_high": support_upper}


def compute_support_zone_from_profile(vp: pd.DataFrame, threshold_ratio: float = 0.60):
    """高出来高ゾーン（POC周辺）を抽出し、下限を支持線として返す。

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


def compute_poc_support_from_profile(vp: pd.DataFrame):
    """期間内で最も出来高が厚い価格帯（POC）の“下側”を下値ラインとして返す。

    ユーザー要望：
      - 1ヶ月表示→直近1ヶ月で最大出来高帯の下限
      - 3ヶ月表示→直近3ヶ月で最大出来高帯の下限
      - 6ヶ月表示→直近6ヶ月で最大出来高帯の下限
      - 1年表示→直近1年で最大出来高帯の下限
    """

    if vp is None or vp.empty or "volume" not in vp.columns:
        return None
    try:
        idx = int(vp["volume"].idxmax())
        return float(vp.loc[idx, "price_low"])
    except Exception:
        return None


def calculate_flow_state(df: pd.DataFrame, avg_volume: int = 0) -> dict:
    """
    チャート用のFlow状態を計算
    """
    if df.empty or len(df) < 20:
        return {"state": "沈静", "absorption_days": []}

    # 出来高倍率を計算
    if avg_volume > 0:
        df = df.copy()
        df["volume_ratio"] = df["Volume"] / avg_volume
    else:
        avg_vol = df["Volume"].mean()
        df = df.copy()
        df["volume_ratio"] = df["Volume"] / avg_vol if avg_vol > 0 else 1.0

    # 価格変動率（絶対値）
    df["price_change"] = abs(df["Close"].pct_change()) * 100

    # FlowScore日（出来高増加 & 価格安定）を検出
    absorption_days = []
    for i in range(1, len(df)):
        vol_ratio = df["volume_ratio"].iloc[i]
        price_change = df["price_change"].iloc[i]

        # 吸収条件: 出来高1.3倍以上 & 価格変動2%以下
        if vol_ratio >= 1.3 and price_change <= 2.0:
            absorption_days.append(df.index[i])

    return {"state": "吸収" if len(absorption_days) >= 2 else "観測中", "absorption_days": absorption_days}


def create_chart(ticker: str, name: str, period: str = "6mo", avg_volume: int = 0, flow_data: dict = None) -> go.Figure:
    """
    ローソク足チャート・出来高・価格帯別売買高を作成
    TradingView風の明るいデザイン（抵抗線・支持線なし）
    """
    df = fetch_chart_data(ticker, period)

    if df.empty:
        fig = go.Figure()
        fig.add_annotation(text="データを取得できませんでした", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        return fig

    # Flow状態を計算
    flow_state = calculate_flow_state(df, avg_volume)
    absorption_days = flow_state.get("absorption_days", [])

    # 出来高の色分け用データ
    avg_vol = df["Volume"].tail(60).mean() if len(df) >= 60 else df["Volume"].mean()

    # 価格帯別売買高を計算（期間ごと）
    bins_map = {"1mo": 30, "3mo": 40, "6mo": 50, "1y": 60}
    volume_profile = calculate_volume_profile(df, bins=bins_map.get(period, 40))

    # 下値ライン（「直近で伸びた帯 × 安値付近」）
    support_price = None
    support_upper = None
    highlight_range = None
    try:
        support_price, support_upper, highlight_range = compute_support_from_recent_growth(
            df,
            bins=bins_map.get(period, 40),
            recent_ratio=0.33,
            low_band_ratio=0.35,
        )
    except Exception:
        support_price, support_upper, highlight_range = None, None, None

    # フォールバック（POC周辺）
    if support_price is None:
        support_price, support_upper = compute_support_zone_from_profile(volume_profile, threshold_ratio=0.60)
        highlight_range = None

    # サブプロット作成
    fig = make_subplots(
        rows=2,
        cols=2,
        column_widths=[0.88, 0.12],
        row_heights=[0.65, 0.35],
        specs=[[{"rowspan": 1}, {"rowspan": 1}], [{"rowspan": 1}, None]],
        shared_xaxes=True,
        vertical_spacing=0.05,
        horizontal_spacing=0.02,
    )

    # ===== ローソク足チャート =====
    fig.add_trace(
        go.Candlestick(
            x=df.index,
            open=df["Open"],
            high=df["High"],
            low=df["Low"],
            close=df["Close"],
            increasing_line_color="#26A69A",
            decreasing_line_color="#EF5350",
            increasing_fillcolor="#26A69A",
            decreasing_fillcolor="#EF5350",
            name="価格",
        ),
        row=1,
        col=1,
    )

    # 下値ラインを描画（見える化：売買指示ではなく位置の目安）
    if support_price is not None:
        try:
            fig.add_hline(y=support_price, line_dash="dot", line_width=1, line_color="#1E88E5", row=1, col=1)
            period_label = {"1mo": "1ヶ月", "3mo": "3ヶ月", "6mo": "6ヶ月", "1y": "1年"}.get(period, period)
            fig.add_annotation(
                x=df.index[-1],
                y=support_price,
                text=f"下値ライン（{period_label}） {support_price:,.0f}",
                showarrow=False,
                xanchor="right",
                yanchor="bottom",
                font=dict(size=10, color="#1E88E5"),
                row=1,
                col=1,
            )
        except Exception:
            pass

    # 「要監視ポイント」マーカー（出来高増 × 値動き小の条件一致日）
    if absorption_days:
        xs, ys = [], []
        for abs_day in absorption_days:
            if abs_day in df.index:
                xs.append(abs_day)
                ys.append(float(df.loc[abs_day, "High"]) * 1.01)
        if xs:
            fig.add_trace(
                go.Scatter(
                    x=xs,
                    y=ys,
                    mode="markers",
                    marker=dict(size=10, color="#7E57C2", symbol="circle"),
                    name="要監視ポイント",
                    hovertemplate="要監視ポイント<br>%{x|%Y-%m-%d}<br>出来高増 × 値動き小<extra></extra>",
                ),
                row=1,
                col=1,
            )

    # ===== 出来高バー =====
    colors = []
    for _, row in df.iterrows():
        vol_ratio = row["Volume"] / avg_vol if avg_vol > 0 else 1
        price_change = abs(row["Close"] / row["Open"] - 1) * 100 if row["Open"] > 0 else 0

        if vol_ratio >= 1.5 and price_change <= 1.5:
            colors.append("#7E57C2")  # 紫（要監視）
        elif vol_ratio >= 1.5:
            colors.append("#FF7043")  # オレンジ（出来高増）
        elif vol_ratio >= 1.2:
            colors.append("#FFB74D")
        else:
            colors.append("#90A4AE")

    fig.add_trace(
        go.Bar(x=df.index, y=df["Volume"], marker_color=colors, marker_line_width=0, name="出来高", opacity=0.9),
        row=2,
        col=1,
    )

    # ===== 価格帯別売買高（右：上段のみ）=====
    if not volume_profile.empty:
        max_vol = volume_profile["volume"].max()
        vp_colors = []
        hl_low = hl_high = None
        if highlight_range:
            hl_low = float(highlight_range.get("price_low"))
            hl_high = float(highlight_range.get("price_high"))

        for _, row in volume_profile.iterrows():
            intensity = row["volume"] / max_vol if max_vol > 0 else 0
            r = int(126 + (63 - 126) * intensity)
            g = int(87 + (81 - 87) * intensity)
            b = int(194 + (181 - 194) * intensity)
            color = f"rgba({r}, {g}, {b}, 0.7)"

            # 下値ラインの根拠となった価格帯を強調
            try:
                if hl_low is not None and hl_high is not None:
                    if abs(float(row.get("price_low")) - hl_low) < 1e-9 and abs(float(row.get("price_high")) - hl_high) < 1e-9:
                        color = "rgba(30, 136, 229, 0.95)"
            except Exception:
                pass

            vp_colors.append(color)

        fig.add_trace(
            go.Bar(
                y=volume_profile["price"],
                x=volume_profile["volume"],
                orientation="h",
                marker_color=vp_colors,
                marker_line_width=0,
                name="価格帯別売買高",
            ),
            row=1,
            col=2,
        )

    # ===== レイアウト設定 =====
    fig.update_layout(
        title_text="",
        height=500,
        showlegend=False,
        paper_bgcolor="#FFFFFF",
        plot_bgcolor="#FAFAFA",
        font=dict(family="Arial, sans-serif", size=11, color="#333333"),
        margin=dict(l=10, r=10, t=30, b=30),
        xaxis_rangeslider_visible=False,
    )

    # ローソク足エリア
    fig.update_yaxes(title_text="", gridcolor="#E8E8E8", showgrid=True, zeroline=False, side="right", tickfont=dict(size=10), row=1, col=1)
    fig.update_xaxes(gridcolor="#E8E8E8", showgrid=True, zeroline=False, showticklabels=False, row=1, col=1)

    # 出来高エリア
    fig.update_yaxes(title_text="", gridcolor="#D0D0D0", showgrid=True, zeroline=False, tickfont=dict(size=9), row=2, col=1)
    fig.update_xaxes(gridcolor="#D0D0D0", showgrid=True, zeroline=False, tickfont=dict(size=9), row=2, col=1)

    fig.add_shape(
        type="rect",
        xref="paper",
        yref="paper",
        x0=0,
        y0=0,
        x1=0.88,
        y1=0.35,
        fillcolor="rgba(240, 244, 248, 0.8)",
        line=dict(width=0),
        layer="below",
    )

    # 右VP：ローソク足の価格軸と連動（y軸一致）
    fig.update_yaxes(matches="y", showticklabels=False, showgrid=False, row=1, col=2)
    fig.update_xaxes(showticklabels=False, showgrid=False, row=1, col=2)

    return fig


def format_volume_pct(v) -> str:
    """出来高/総発行株数（%）の表示用フォーマット。

    - データがない場合は "-"
    - 0.01%未満は "<0.01%" 表示
    """
    if v is None:
        return "-"
    try:
        fv = float(v)
        if not np.isfinite(fv):
            return "-"
        if fv < 0.01:
            return "<0.01%"
        if fv < 1:
            return f"{fv:.2f}%"
        if fv < 10:
            return f"{fv:.1f}%"
        return f"{fv:.0f}%"
    except Exception:
        return "-"


def render_card(ticker: str, d: Dict, show_cap_badge: bool = False):
    """銘柄カードを表示（LEVEL + FlowScore + 状態タグ）"""
    flow_score = d.get("flow_score", 0)
    level = int(d.get("level", 0))
    ma_score = d.get("ma_score", None)
    state = d.get("display_state", d.get("state", "観測中"))
    tags = d.get("tags", [])

    if flow_score >= FLOW_SCORE_HIGH:
        card_class = "high"
        score_class = "high"
    elif flow_score >= FLOW_SCORE_MEDIUM:
        card_class = "medium"
        score_class = "medium"
    else:
        card_class = ""
        score_class = "normal"

    level_color = LEVEL_COLORS.get(level, "#9E9E9E")

    code = ticker.replace(".T", "")
    url = f"https://finance.yahoo.co.jp/quote/{code}.T"
    name_jp = TICKER_NAMES_JP.get(ticker, d.get("name", code))

    # タグ表示（最大4つ）
    tags_html = ""
    _tag_prefix_re = re.compile(r"^[\s\u3000]*[○●■★※・▶▷▲▼◇◆□◆◆✦✧✱✳︎⭐️]+")
    def _norm(t: str) -> str:
        if t is None:
            return ""
        return _tag_prefix_re.sub("", str(t)).strip()

    for raw_tag in (tags or [])[:4]:
        tag = _norm(raw_tag) or str(raw_tag)
        if tag == "要監視":
            tags_html += '<span style="background:#E8EAF6;color:#5C6BC0;padding:2px 8px;border-radius:999px;font-size:0.65rem;margin-right:6px;font-weight:700;">要監視</span>'
        elif tag == "下側ゾーン":
            tags_html += '<span style="background:#E3F2FD;color:#1565C0;padding:2px 8px;border-radius:999px;font-size:0.65rem;margin-right:6px;font-weight:700;">下側ゾーン</span>'
        elif tag == "上側ゾーン":
            tags_html += '<span style="background:#FFEBEE;color:#C62828;padding:2px 8px;border-radius:999px;font-size:0.65rem;margin-right:6px;font-weight:700;">上側ゾーン</span>'
        else:
            tags_html += f'<span style="background:#F3F4F6;color:#444;padding:2px 8px;border-radius:999px;font-size:0.65rem;margin-right:6px;">{tag}</span>'

    st.markdown(
        f"""
    <div class="stock-card {card_class}">
        <div class="stock-header">
            <div>
                <a href="{url}" target="_blank" class="stock-title">{ticker.replace('.T','')} {name_jp}</a>
                <div class="stock-sub">¥{d.get('price',0):,.0f} / FlowScore {flow_score}</div>
            </div>
            <div class="level-badge" style="background:{level_color};">LEVEL {level}</div>
        </div>
        <div class="stock-info">
            <div><span class="info-label">状態</span><br><span class="info-value">{state}</span></div>
            <div><span class="info-label">時価総額</span><br><span class="info-value">{d.get('market_cap_oku',0):,}億円</span></div>
            <div>
                <span class="info-label">出来高</span><br>
                <span class="info-value">{d.get('vol_ratio', 0)}x</span>
                <div style="margin-top:2px;font-size:0.72rem;color:#64748B;font-weight:700;">
                    株数比 {format_volume_pct(d.get('volume_of_shares_pct'))}
                </div>
            </div>
        </div>
        <div style="padding:0 0.8rem 0.5rem;font-size:0.7rem;">{tags_html}</div>
    </div>
    """,
        unsafe_allow_html=True,
    )

    chart_open = st.session_state.setdefault("chart_open", {})
    is_open = bool(chart_open.get(ticker, False))

    toggle_label = "📊 チャートを閉じる" if is_open else "📊 チャートを表示"
    if st.button(toggle_label, key=f"chart_{ticker}", use_container_width=True):
        chart_open[ticker] = not is_open
        st.session_state["chart_open"] = chart_open
        st.rerun()

    if chart_open.get(ticker, False):
        period = st.session_state.get("chart_period", "6mo")
        fig = create_chart(ticker, name_jp, period=period, avg_volume=d.get("avg_volume", 0), flow_data=d)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        st.caption("● 要監視ポイント：出来高増 × 値動き小 の条件が一致した日（売買の合図ではなく、状態の記録）")
        st.caption("※表示は市場データの可視化を目的とした補助情報です。")


# ==========================================
# UI
# ==========================================
def main_app():
    st.set_page_config(page_title="ハゲタカSCOPE（テスト）", layout="wide")

    st.title("🦅 ハゲタカSCOPE（テスト環境）")
    st.caption("※本ツールは市場データの可視化を目的とした補助ツールです。銘柄推奨・売買助言ではありません。")

    data = load_data()
    updated_at = data.get("updated_at", "")

    tab1, tab2 = st.tabs(["📌 候補一覧", "🔔 通知設定"])

    with tab1:
        if data and ("data" in data):
            st.caption(f"更新: {updated_at} JST")

            guide_md = """
**LEVELは“条件一致の多さ”を示す目安（1〜4）です。**  
本ツールは市場データの可視化であり、銘柄推奨・売買助言ではありません。

- **LEVEL 1**：条件に触れた（変化は小さい）
- **LEVEL 2**：変化が見え始めた
- **LEVEL 3**：複数条件が重なっている
- **LEVEL 4**：条件一致が多く密度が高い

**要監視**：取引量が増えているのに値動きが小さい等、状態が“目立つ”ときに表示されます（理由は複数あります）。
"""
            try:
                with st.popover("❓LEVEL", use_container_width=False):
                    st.markdown(guide_md)
            except Exception:
                with st.expander("❓LEVELガイド", expanded=False):
                    st.markdown(guide_md)

            st.markdown(
                """
            <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                        border-radius: 10px; padding: 0.8rem; margin-bottom: 1rem; color: white; font-size: 0.8rem;">
                <strong>🎯 候補抽出の考え方</strong><br>
                「取引量が増えているのに値動きが小さい」などの条件一致を組み合わせ、候補を少数に絞って表示します。
            </div>
            """,
                unsafe_allow_html=True,
            )

            show_all = st.checkbox("中型株以外も表示", value=False, help="通常は時価総額300〜2000億円の中型株のみを表示します。ONにすると対象外も含めて表示します。")

            if show_all:
                display_data = data.get("all_data", {})
            else:
                display_data = data.get("data", {})

            lvl4 = len([v for v in display_data.values() if int(v.get("level", 0)) == 4])
            lvl3p = len([v for v in display_data.values() if int(v.get("level", 0)) >= 3])

            col1, col2, col3 = st.columns(3)
            with col1:
                st.markdown(f'<div class="stat-box"><div class="stat-value high">{lvl4}</div><div class="stat-label">LEVEL 4</div></div>', unsafe_allow_html=True)
            with col2:
                st.markdown(f'<div class="stat-box"><div class="stat-value medium">{lvl3p}</div><div class="stat-label">LEVEL 3+</div></div>', unsafe_allow_html=True)
            with col3:
                st.markdown(f'<div class="stat-box"><div class="stat-value total">{len(display_data)}</div><div class="stat-label">表示件数</div></div>', unsafe_allow_html=True)

            st.markdown("")

            # 絞り込み（フィルターパネル）
            if "flt_levels" not in st.session_state:
                st.session_state["flt_levels"] = []
            if "flt_zones" not in st.session_state:
                st.session_state["flt_zones"] = []
            if "flt_watch_only" not in st.session_state:
                st.session_state["flt_watch_only"] = False
            if "flt_query" not in st.session_state:
                st.session_state["flt_query"] = ""

            _TAG_PREFIX_RE = re.compile(r"^[\s\u3000]*[○●■★※・▶▷▲▼◇◆□◆◆✦✧✱✳︎⭐️]+")
            def _norm_tag(t: str) -> str:
                if t is None:
                    return ""
                s = str(t).strip()
                s = _TAG_PREFIX_RE.sub("", s).strip()
                return s

            def _norm_tags(item: dict) -> set:
                tags = item.get("tags") or []
                return {_norm_tag(t) for t in tags if _norm_tag(t)}

            def _is_watch(item: dict) -> bool:
                stt = (item.get("display_state") or item.get("state") or "").strip()
                if stt == "要監視":
                    return True
                n_tags = _norm_tags(item)
                return ("要監視" in n_tags) or any("要監視" in t for t in n_tags)

            def _apply_filters(items: dict) -> dict:
                q = (st.session_state.get("flt_query") or "").strip().lower()
                levels = st.session_state.get("flt_levels") or []
                zones = st.session_state.get("flt_zones") or []
                watch_only = bool(st.session_state.get("flt_watch_only"))

                out = {}
                for tk, it in items.items():
                    if levels:
                        try:
                            if str(int(it.get("level", 0))) not in set(levels):
                                continue
                        except Exception:
                            continue

                    if zones:
                        tags = _norm_tags(it)
                        if not any(z in tags for z in zones):
                            continue

                    if watch_only and not _is_watch(it):
                        continue

                    if q:
                        name = it.get("name") or ""
                        hay = f"{tk} {name}".lower()
                        if q not in hay:
                            continue

                    out[tk] = it
                return out

            tb1, tb2, tb3 = st.columns([1.2, 0.9, 2.9])
            with tb1:
                def _render_filter_ui():
                    st.markdown("#### 絞り込み")
                    st.session_state["flt_query"] = st.text_input(
                        "検索（銘柄名・コード）",
                        value=st.session_state.get("flt_query", ""),
                        placeholder="例：7203 / トヨタ",
                        label_visibility="visible",
                    )

                    st.markdown("**LEVEL**")
                    st.session_state["flt_levels"] = st.multiselect(
                        " ",
                        options=["4", "3", "2", "1"],
                        default=st.session_state.get("flt_levels") or [],
                    )

                    st.markdown("**ゾーン**")
                    st.session_state["flt_zones"] = st.multiselect(
                        "  ",
                        options=["下側ゾーン", "上側ゾーン"],
                        default=st.session_state.get("flt_zones") or [],
                    )

                    st.session_state["flt_watch_only"] = st.toggle(
                        "要監視のみ",
                        value=bool(st.session_state.get("flt_watch_only")),
                    )

                try:
                    with st.popover("🔎 フィルター"):
                        _render_filter_ui()
                except Exception:
                    with st.expander("🔎 フィルター", expanded=False):
                        _render_filter_ui()

            with tb2:
                if st.button("🔄 リセット", use_container_width=True):
                    st.session_state["flt_levels"] = []
                    st.session_state["flt_zones"] = []
                    st.session_state["flt_watch_only"] = False
                    st.session_state["flt_query"] = ""
                    st.rerun()

            with tb3:
                chips = []
                if st.session_state.get("flt_levels"):
                    chips.append("LEVEL: " + ",".join(st.session_state["flt_levels"]))
                if st.session_state.get("flt_zones"):
                    chips.append(" / ".join(st.session_state["flt_zones"]))
                if st.session_state.get("flt_watch_only"):
                    chips.append("要監視")
                if st.session_state.get("flt_query"):
                    chips.append("検索: " + st.session_state["flt_query"])
                if chips:
                    st.caption("  ・  ".join(chips))

            display_data = _apply_filters(display_data)

            if display_data:
                for ticker, d in sorted(display_data.items(), key=lambda x: (int(x[1].get("level", 0)), float(x[1].get("ma_score", 0)), float(x[1].get("flow_score", 0))), reverse=True):
                    render_card(ticker, d)
            else:
                st.info("該当する銘柄がありません")
        else:
            st.info("データがありません（GitHub Actionsで生成してください）")

    with tab2:
        st.markdown("### 🔔 メール通知設定")
        st.markdown('<p style="color:#666;font-size:0.8rem;">出来高急動（1.5倍以上）を検知した際に通知を受け取れます</p>', unsafe_allow_html=True)

        current_email = st.session_state.get("email_address", "")
        if current_email:
            st.markdown(
                f"""
            <div style="background:#E8F5E9;border-radius:8px;padding:0.5rem 1rem;margin-bottom:1rem;font-size:0.85rem;">
                ✅ ログイン中: <strong>{current_email}</strong>
            </div>
            """,
                unsafe_allow_html=True,
            )

        email = st.text_input("Gmailアドレス", value=st.session_state.get("email_address", ""), placeholder="example@gmail.com")
        app_password = st.text_input("アプリパスワード（16桁）", value=st.session_state.get("app_password", ""), type="password", placeholder="xxxx xxxx xxxx xxxx")

        col1, col2 = st.columns(2)

        with col1:
            if st.button("💾 保存", use_container_width=True):
                if not email:
                    st.warning("⚠️ メールアドレスを入力してください")
                elif "@" not in email:
                    st.warning("⚠️ 正しいメールアドレスを入力してください")
                elif not app_password:
                    st.warning("⚠️ アプリパスワードを入力してください")
                else:
                    with st.spinner("保存中..."):
                        if save_settings_to_sheet(email, app_password):
                            st.session_state["email_address"] = email
                            st.session_state["app_password"] = app_password
                            st.success("✅ 保存しました")
                        else:
                            st.error("❌ 保存に失敗しました（設定/権限をご確認ください）")

        with col2:
            if st.button("🔎 登録確認", use_container_width=True):
                if not email:
                    st.warning("⚠️ メールアドレスを入力してください")
                else:
                    if is_registered_email(email):
                        st.success("✅ 登録済みです")
                    else:
                        st.info("未登録です（保存すると登録されます）")


if __name__ == "__main__":
    main_app()
