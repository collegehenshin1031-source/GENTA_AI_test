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
    "7735.T": "SCREENホールディングス",
    "6146.T": "ディスコ",
    "6266.T": "タツモ",
    "3132.T": "マクニカホールディングス",
    "6920.T": "レーザーテック",
    "4565.T": "そーせいグループ",
    "4587.T": "ペプチドリーム",
    "4582.T": "シンバイオ製薬",
    "4583.T": "カイオム・バイオ",
    "4563.T": "アンジェス",
    "2370.T": "メディネット",
    "4593.T": "ヘリオス",
    "3064.T": "MonotaRO",
    "3092.T": "ZOZO",
    "3769.T": "GMOペイメント",
    "4385.T": "メルカリ",
    "7342.T": "ウェルスナビ",
    "4480.T": "メドレー",
    "6560.T": "LTS",
    "3182.T": "オイシックス",
    "9166.T": "GENDA",
    "3765.T": "ガンホー",
    "3659.T": "ネクソン",
    "3656.T": "KLab",
    "3932.T": "アカツキ",
    "4071.T": "プラスアルファ",
    "4485.T": "JTOWER",
    "7095.T": "Macbee Planet",
    "4054.T": "日本情報クリエイト",
    "6095.T": "メドピア",
    "4436.T": "ミンカブ",
    "4477.T": "BASE",
}

# ==========================================
# UI設定
# ==========================================
st.set_page_config(
    page_title="源太AI🤖ハゲタカSCOPE", 
    page_icon="🦅", 
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ==========================================
# CSS
# ==========================================
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800&display=swap');

#MainMenu, footer, header, .stDeployButton {display: none !important;}

html, body, [class*="css"]  { font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif !important; }

/* App background */
div[data-testid="stAppViewContainer"]{
  background: radial-gradient(1200px 600px at 10% 0%, rgba(92,107,192,0.10), transparent 60%),
              radial-gradient(900px 450px at 95% 10%, rgba(196,30,58,0.10), transparent 55%),
              linear-gradient(180deg, #FFFFFF 0%, #FAFBFF 60%, #FFFFFF 100%) !important;
}

.main .block-container{
  max-width: 1080px !important;
  padding: 1.0rem 1.2rem 3.2rem 1.2rem !important;
}

/* Top title */
h1{
  text-align:center !important;
  font-size: 1.55rem !important;
  font-weight: 800 !important;
  letter-spacing: -0.02em !important;
  color: #0F172A !important;
  margin-bottom: .2rem !important;
}
.subtitle{
  text-align:center;
  color:#64748B;
  font-size:.85rem;
  margin-bottom: 1.1rem;
}

/* Tabs */
.stTabs [data-baseweb="tab-list"]{
  justify-content:center !important;
  background: rgba(255,255,255,0.75) !important;
  backdrop-filter: blur(8px);
  padding: .35rem !important;
  border-radius: 14px !important;
  border: 1px solid rgba(15,23,42,0.08) !important;
  box-shadow: 0 10px 30px rgba(15,23,42,0.06) !important;
  margin-bottom: 1.0rem !important;
}
.stTabs [data-baseweb="tab"]{
  padding: .55rem 1.05rem !important;
  border-radius: 10px !important;
  font-weight: 700 !important;
  color: #475569 !important;
}
.stTabs [data-baseweb="tab"][aria-selected="true"]{
  background: linear-gradient(135deg, #0F172A 0%, #334155 100%) !important;
}
.stTabs [data-baseweb="tab"][aria-selected="true"] p{
  color: #FFFFFF !important;
}

/* Cards */
.spike-card{
  background: rgba(255,255,255,0.88);
  backdrop-filter: blur(8px);
  border-radius: 16px;
  padding: 1rem;
  margin-bottom: .75rem;
  border: 1px solid rgba(15,23,42,0.08);
  box-shadow: 0 14px 40px rgba(15,23,42,0.08);
}
.spike-card.high{
  border-color: rgba(196,30,58,0.22);
  box-shadow: 0 16px 44px rgba(196,30,58,0.12);
}
.spike-card.medium{
  border-color: rgba(255,152,0,0.22);
  box-shadow: 0 16px 44px rgba(255,152,0,0.10);
}

.card-header{
  display:flex;
  justify-content:space-between;
  align-items:center;
  gap:.7rem;
  margin-bottom: .55rem;
}

.ticker-name a{
  font-weight: 800;
  color:#0F172A !important;
  text-decoration:none;
}
.ticker-name a:hover{ text-decoration: underline; }

.ratio-badge{
  min-width: 46px;
  text-align:center;
  padding: .28rem .6rem;
  border-radius: 999px;
  font-weight: 800;
  font-size: .82rem;
  border: 1px solid rgba(15,23,42,0.10);
  color:#0F172A;
  background: rgba(255,255,255,0.8);
}
.ratio-badge.high{
  color:#FFFFFF;
  border-color: rgba(196,30,58,0.25);
  background: linear-gradient(135deg, #C41E3A 0%, #E63946 100%);
}
.ratio-badge.medium{
  color:#0F172A;
  border-color: rgba(255,152,0,0.25);
  background: linear-gradient(135deg, rgba(255,152,0,0.18) 0%, rgba(255,193,7,0.18) 100%);
}

.card-body{
  display:grid;
  grid-template-columns: repeat(4, 1fr);
  gap: .8rem;
  margin-top: .2rem;
}

.info-label{
  font-size: .72rem;
  color:#64748B;
  font-weight: 700;
  letter-spacing: .02em;
}
.info-value{
  font-size: .93rem;
  color:#0F172A;
  font-weight: 700;
}

/* Stat boxes */
.stat-box{
  background: rgba(255,255,255,0.88);
  border: 1px solid rgba(15,23,42,0.08);
  border-radius: 14px;
  padding: .9rem .9rem;
  box-shadow: 0 10px 30px rgba(15,23,42,0.06);
  text-align:center;
}
.stat-value{
  font-size: 1.55rem;
  font-weight: 900;
  line-height: 1.1;
}
.stat-value.high{ color:#C41E3A; }
.stat-value.medium{ color:#FF9800; }
.stat-value.total{ color:#0F172A; }
.stat-label{ color:#64748B; font-size:.78rem; font-weight:700; margin-top:.25rem; }

/* Pill buttons (LEVEL filter + chart period) */
.pill-row{ display:flex; gap:.45rem; flex-wrap:wrap; }
.pill{
  display:inline-flex;
  align-items:center;
  justify-content:center;
  padding:.42rem .75rem;
  border-radius: 999px;
  font-weight: 800;
  font-size:.78rem;
  border: 1px solid rgba(15,23,42,0.14);
  background: rgba(255,255,255,0.75);
  color:#334155;
}
.pill.active{
  background: linear-gradient(135deg, #0F172A 0%, #334155 100%);
  border-color: rgba(15,23,42,0.0);
  color:#FFFFFF;
}
.pill.soft{
  background: rgba(92,107,192,0.10);
  border-color: rgba(92,107,192,0.22);
  color:#3F51B5;
}

/* Sticky guide */
.sticky-guide{position: sticky; top: .6rem; z-index: 50; margin-bottom: .6rem;}

/* Buttons */
div.stButton > button{
  border-radius: 12px !important;
  font-weight: 800 !important;
  padding: .55rem .9rem !important;
}
</style>
""", unsafe_allow_html=True)


# ==========================================
# チャート関連関数
# ==========================================
@st.cache_data(ttl=300)
def fetch_chart_data(ticker: str, period: str = "6mo") -> pd.DataFrame:
    """チャート用の株価データを取得"""
    try:
        stock = yf.Ticker(ticker)
        df = stock.history(period=period)
        return df
    except Exception as e:
        st.error(f"データ取得エラー: {e}")
        return pd.DataFrame()


def calculate_volume_profile(df: pd.DataFrame, bins: int = 20) -> pd.DataFrame:
    """価格帯別売買高を計算"""
    if df.empty:
        return pd.DataFrame()
    
    # 価格範囲を分割
    price_min = df['Low'].min()
    price_max = df['High'].max()
    price_bins = np.linspace(price_min, price_max, bins + 1)
    
    volume_profile = []
    
    for i in range(len(price_bins) - 1):
        bin_low = price_bins[i]
        bin_high = price_bins[i + 1]
        bin_center = (bin_low + bin_high) / 2
        
        # この価格帯に含まれる日の出来高を集計
        total_volume = 0
        for _, row in df.iterrows():
            if row['Low'] <= bin_high and row['High'] >= bin_low:
                # 出来高を価格帯に按分
                overlap_low = max(row['Low'], bin_low)
                overlap_high = min(row['High'], bin_high)
                if row['High'] > row['Low']:
                    ratio = (overlap_high - overlap_low) / (row['High'] - row['Low'])
                else:
                    ratio = 1.0
                total_volume += row['Volume'] * ratio
        
        volume_profile.append({
            'price': bin_center,
            'price_low': bin_low,
            'price_high': bin_high,
            'volume': total_volume
        })
    
    return pd.DataFrame(volume_profile)


def calculate_flow_state(df: pd.DataFrame, avg_volume: int = 0) -> dict:
    """
    チャート用のFlow状態を計算
    """
    if df.empty or len(df) < 20:
        return {"state": "沈静", "absorption_days": []}
    
    # 出来高倍率を計算
    if avg_volume > 0:
        df = df.copy()
        df['volume_ratio'] = df['Volume'] / avg_volume
    else:
        avg_vol = df['Volume'].mean()
        df = df.copy()
        df['volume_ratio'] = df['Volume'] / avg_vol if avg_vol > 0 else 1.0
    
    # 価格変動率（絶対値）
    df['price_change'] = abs(df['Close'].pct_change()) * 100
    
    # FlowScore日（出来高増加 & 価格安定）を検出
    absorption_days = []
    for i in range(1, len(df)):
        vol_ratio = df['volume_ratio'].iloc[i]
        price_change = df['price_change'].iloc[i]
        
        # 吸収条件: 出来高1.3倍以上 & 価格変動2%以下
        if vol_ratio >= 1.3 and price_change <= 2.0:
            absorption_days.append(df.index[i])
    
    return {
        "state": "吸収" if len(absorption_days) >= 2 else "観測中",
        "absorption_days": absorption_days
    }


def create_chart(ticker: str, name: str, period: str = "6mo", avg_volume: int = 0, flow_data: dict = None) -> go.Figure:
    """
    ローソク足チャート・出来高・価格帯別売買高を作成
    TradingView風の明るいデザイン（抵抗線・支持線なし）
    """
    df = fetch_chart_data(ticker, period)
    
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(text="データを取得できませんでした", 
                          xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        return fig
    
    # Flow状態を計算
    flow_state = calculate_flow_state(df, avg_volume)
    absorption_days = flow_state.get("absorption_days", [])
    
    # 出来高の色分け用データ
    avg_vol = df['Volume'].tail(60).mean() if len(df) >= 60 else df['Volume'].mean()
    
    # 価格帯別売買高を計算
    volume_profile = calculate_volume_profile(df)
    
    # サブプロット作成
    fig = make_subplots(
        rows=2, cols=2,
        column_widths=[0.88, 0.12],
        row_heights=[0.65, 0.35],
        specs=[[{"rowspan": 1}, {"rowspan": 2}],
               [{"rowspan": 1}, None]],
        shared_xaxes=True,
        vertical_spacing=0.05,
        horizontal_spacing=0.02
    )
    
    # ===== ローソク足チャート =====
    fig.add_trace(
        go.Candlestick(
            x=df.index,
            open=df['Open'],
            high=df['High'],
            low=df['Low'],
            close=df['Close'],
            increasing_line_color='#26A69A',
            decreasing_line_color='#EF5350',
            increasing_fillcolor='#26A69A',
            decreasing_fillcolor='#EF5350',
            name='価格'
        ),
        row=1, col=1
    )
    
    # FlowScore日にマーカーを追加（○記号）
    for abs_day in absorption_days:
        if abs_day in df.index:
            fig.add_annotation(
                x=abs_day,
                y=df.loc[abs_day, 'High'] * 1.01,
                text="○",
                showarrow=False,
                font=dict(size=12, color="#7E57C2"),
                row=1, col=1
            )
    
    # ===== 出来高バー =====
    colors = []
    for idx, row in df.iterrows():
        vol_ratio = row['Volume'] / avg_vol if avg_vol > 0 else 1
        price_change = abs(row['Close'] / row['Open'] - 1) * 100 if row['Open'] > 0 else 0
        
        # FlowScore（出来高増 & 価格安定）
        if vol_ratio >= 1.5 and price_change <= 1.5:
            colors.append('#7E57C2')  # 紫（FlowScore）
        elif vol_ratio >= 1.5:
            colors.append('#FF7043')  # オレンジ（出来高増）
        elif vol_ratio >= 1.2:
            colors.append('#FFB74D')  # 薄いオレンジ
        else:
            colors.append('#90A4AE')  # グレー（通常）
    
    fig.add_trace(
        go.Bar(
            x=df.index,
            y=df['Volume'],
            marker_color=colors,
            marker_line_width=0,
            name='出来高',
            opacity=0.9
        ),
        row=2, col=1
    )
    
    # ===== 価格帯別売買高 =====
    if not volume_profile.empty:
        max_vol = volume_profile['volume'].max()
        vp_colors = []
        for _, row in volume_profile.iterrows():
            intensity = row['volume'] / max_vol if max_vol > 0 else 0
            r = int(126 + (63 - 126) * intensity)
            g = int(87 + (81 - 87) * intensity)
            b = int(194 + (181 - 194) * intensity)
            vp_colors.append(f'rgba({r}, {g}, {b}, 0.7)')
        
        fig.add_trace(
            go.Bar(
                y=volume_profile['price'],
                x=volume_profile['volume'],
                orientation='h',
                marker_color=vp_colors,
                marker_line_width=0,
                name='価格帯別売買高'
            ),
            row=1, col=2
        )
    
    # ===== レイアウト設定 =====
    fig.update_layout(
        title=None,
        height=500,
        showlegend=False,
        paper_bgcolor='#FFFFFF',
        plot_bgcolor='#FAFAFA',
        font=dict(family="Arial, sans-serif", size=11, color='#333333'),
        margin=dict(l=10, r=10, t=30, b=30),
        xaxis_rangeslider_visible=False,
    )
    
    # ローソク足エリア
    fig.update_yaxes(
        title_text="",
        gridcolor='#E8E8E8',
        showgrid=True,
        zeroline=False,
        side='right',
        tickfont=dict(size=10),
        row=1, col=1
    )
    fig.update_xaxes(
        gridcolor='#E8E8E8',
        showgrid=True,
        zeroline=False,
        showticklabels=False,
        row=1, col=1
    )
    
    # 出来高エリア（背景色で差別化）
    fig.update_yaxes(
        title_text="",
        gridcolor='#D0D0D0',
        showgrid=True,
        zeroline=False,
        tickfont=dict(size=9),
        row=2, col=1
    )
    fig.update_xaxes(
        gridcolor='#D0D0D0',
        showgrid=True,
        zeroline=False,
        tickfont=dict(size=9),
        row=2, col=1
    )
    
    fig.add_shape(
        type="rect",
        xref="paper", yref="paper",
        x0=0, y0=0, x1=0.88, y1=0.35,
        fillcolor="rgba(240, 244, 248, 0.8)",
        line=dict(width=0),
        layer="below"
    )
    
    # 価格帯別売買高エリア
    fig.update_yaxes(showticklabels=False, showgrid=False, row=1, col=2)
    fig.update_xaxes(showticklabels=False, showgrid=False, row=1, col=2)
    
    return fig


def show_chart_modal(ticker: str, stock_info: dict):
    """チャートモーダルを表示（スマホ対応）"""
    name = stock_info.get("name", ticker)
    avg_volume = stock_info.get("avg_volume", 0)
    flow_score = stock_info.get("flow_score", 0)
    stage = stock_info.get("stage", "監視中")
    state = stock_info.get("state", "観測中")
    flow_details = stock_info.get("flow_details", {})
    
    # ステージ色
    stage_color = STAGE_COLORS.get(stage, "#9E9E9E")
    
    # 戻るボタン
    if st.button("← 一覧に戻る", key="back_btn"):
        st.session_state["show_chart"] = False
        st.session_state["selected_ticker"] = None
        st.rerun()
    
    st.markdown("---")
    
    # 期間選択
    period_cols = st.columns(4)
    periods = [("1ヶ月", "1mo"), ("3ヶ月", "3mo"), ("6ヶ月", "6mo"), ("1年", "1y")]
    for i, (label, period_val) in enumerate(periods):
        with period_cols[i]:
            if st.button(label, use_container_width=True, key=f"period_{period_val}"):
                st.session_state["chart_period"] = period_val
                st.rerun()
    
    period = st.session_state.get("chart_period", "6mo")
    period_labels = {"1mo": "1ヶ月", "3mo": "3ヶ月", "6mo": "6ヶ月", "1y": "1年"}
    
    # 銘柄情報表示（FlowScore対応）
    st.markdown(f"""
    <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                border-radius: 12px; padding: 1rem; margin: 1rem 0; color: white;">
        <div style="text-align: center;">
            <div style="font-size: 1.3rem; font-weight: bold; margin-bottom: 0.3rem;">
                {ticker} {name}
            </div>
            <div style="font-size: 2rem; font-weight: bold;">
                ¥{stock_info.get('price', 0):,.0f}
            </div>
            <div style="display: flex; justify-content: center; gap: 1rem; margin-top: 0.5rem;">
                <div style="background: rgba(255,255,255,0.2); padding: 0.3rem 0.8rem; border-radius: 20px;">
                    FlowScore: <strong>{flow_score}</strong>
                </div>
                <div style="background: {stage_color}; padding: 0.3rem 0.8rem; border-radius: 20px;">
                    {stage}
                </div>
            </div>
            <div style="font-size: 0.8rem; margin-top: 0.3rem; opacity: 0.9;">
                状態: {state} | 期間: {period_labels.get(period, '6ヶ月')}
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # FlowScore詳細（折りたたみ）
    with st.expander("📊 FlowScoreの詳細"):
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("出来高異常度", f"{flow_details.get('vol_anomaly', 0)}")
        with col2:
            st.metric("価格安定度", f"{flow_details.get('price_stability', 0)}")
        with col3:
            st.metric("吸収度", f"{flow_details.get('absorption', 0)}")
    
    # チャート表示
    with st.spinner("チャートを読み込み中..."):
        fig = create_chart(ticker, name, period, avg_volume, flow_details)
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
    
    # 凡例
    st.markdown("""
    <div style="background: #F5F5F5; border-radius: 8px; padding: 0.8rem; font-size: 0.75rem; color: #666; line-height: 1.8;">
        <strong>📊 チャートの見方</strong><br>
        <span style="color: #26A69A;">■</span> 陽線（上昇）　
        <span style="color: #EF5350;">■</span> 陰線（下落）<br>
        <span style="color: #7E57C2;">■</span> 出来高：要監視（取引増&動き小）　
        <span style="color: #FF7043;">■</span> 出来高増　
        <span style="color: #90A4AE;">■</span> 通常<br>
        <span style="color: #7E57C2;">○</span> 要監視日（条件一致）
    </div>
    """, unsafe_allow_html=True)


# ==========================================
# ロゴ画像を読み込み
# ==========================================
def get_logo_base64():
    try:
        with open("logo.png", "rb") as f:
            return base64.b64encode(f.read()).decode()
    except:
        return None


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
    """暗号化されたパスワードを復号化"""
    if not encrypted_password:
        return ""
    try:
        fernet = get_fernet()
        decrypted = fernet.decrypt(encrypted_password.encode())
        return decrypted.decode()
    except Exception:
        return ""


# ==========================================
# Google Sheets連携
# ==========================================
def get_gsheets_connection():
    """Google Sheets接続を取得"""
    return st.connection("gsheets", type=GSheetsConnection)


def load_settings_by_email(email: str) -> Optional[Dict]:
    """メールアドレスでスプレッドシートから設定を読み込み"""
    if not email:
        return None
    
    try:
        conn = get_gsheets_connection()
        # ttl=0でキャッシュを無効化（常に最新データを取得）
        df = conn.read(worksheet="settings", usecols=[0, 1], ttl=0)
        
        if df is None or df.empty:
            return None
        
        # カラム名を正規化
        df.columns = ["email", "encrypted_password"]
        
        # メールアドレスで検索（小文字化・トリム）
        email_normalized = email.lower().strip()
        row = df[df["email"].str.lower().str.strip() == email_normalized]
        
        if row.empty:
            return None
        
        return {
            "email": row.iloc[0]["email"],
            "encrypted_password": row.iloc[0]["encrypted_password"]
        }
    except Exception as e:
        # エラー時はキャッシュをクリアして再試行
        st.cache_data.clear()
        return None


def get_gspread_client():
    """gspreadクライアントを取得（行単位操作用）"""
    try:
        # st.secretsからサービスアカウント情報を取得
        credentials_dict = dict(st.secrets["connections"]["gsheets"])
        
        # spreadsheetキーがあれば除外（認証情報ではないため）
        credentials_dict.pop("spreadsheet", None)
        credentials_dict.pop("worksheet", None)
        
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        
        credentials = Credentials.from_service_account_info(credentials_dict, scopes=scopes)
        client = gspread.authorize(credentials)
        return client
    except Exception as e:
        st.error(f"gspread接続エラー: {str(e)}")
        return None


def save_settings_to_sheet(email: str, app_password: str) -> bool:
    """スプレッドシートに設定を保存（行単位操作でデータ消失防止）"""
    if not email:
        return False
    
    email = email.lower().strip()  # 小文字化＆トリム
    
    try:
        client = get_gspread_client()
        if not client:
            return False
        
        # スプレッドシートを開く
        spreadsheet_url = st.secrets["connections"]["gsheets"].get("spreadsheet")
        if not spreadsheet_url:
            st.error("スプレッドシートURLが設定されていません")
            return False
        
        spreadsheet = client.open_by_url(spreadsheet_url)
        worksheet = spreadsheet.worksheet("settings")
        
        # パスワードを暗号化
        encrypted_pw = encrypt_password(app_password)
        
        # 既存のメールアドレスを検索
        try:
            all_emails = worksheet.col_values(1)  # A列（email列）を取得
        except Exception:
            all_emails = []
        
        # ヘッダー行を考慮（1行目がヘッダーの場合）
        email_found = False
        row_index = -1
        
        for i, cell_email in enumerate(all_emails):
            if cell_email and cell_email.lower().strip() == email:
                email_found = True
                row_index = i + 1  # gspreadは1始まり
                break
        
        if email_found and row_index > 1:  # ヘッダー行（1行目）は除外
            # 既存ユーザー：該当行のパスワード列（B列）を更新
            worksheet.update_cell(row_index, 2, encrypted_pw)
        else:
            # 新規ユーザー：行を追記
            worksheet.append_row([email, encrypted_pw])
        
        # キャッシュをクリア
        st.cache_data.clear()
        
        return True
    except Exception as e:
        st.error(f"保存エラー: {str(e)}")
        return False


# ==========================================
# メール送信
# ==========================================
def send_test_email(email: str, app_password: str) -> tuple[bool, str]:
    try:
        msg = MIMEMultipart()
        msg["From"] = email
        msg["To"] = email
        msg["Subject"] = "🦅 ハゲタカSCOPE - テスト通知"
        body = "メール設定が正常に完了しました！\n\n出来高急動が検知された際に通知が届きます。"
        msg.attach(MIMEText(body, "plain", "utf-8"))
        
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(email, app_password)
            server.send_message(msg)
        return True, "テストメール送信成功！"
    except smtplib.SMTPAuthenticationError:
        return False, "認証エラー: アプリパスワードを確認してください"
    except Exception as e:
        return False, f"送信エラー: {str(e)}"


def send_spike_alert(email: str, app_password: str, stocks: List[Dict], updated_at: str) -> bool:
    """手動メール送信（助言ではなく“候補一覧の共有”）。"""
    if not stocks:
        return False
    try:
        msg = MIMEMultipart()
        msg["From"] = email
        msg["To"] = email
        msg["Subject"] = f"🦅 ハゲタカSCOPE 候補通知: {len(stocks)}件 - {updated_at[:10]}"

        lines = [
            "━" * 30,
            "🦅 ハゲタカSCOPE（候補一覧）",
            "━" * 30,
            f"更新日時: {updated_at}",
            f"表示件数: {len(stocks)}",
            "",
            "※本通知は市場データの可視化に基づく候補一覧です。銘柄推奨・売買助言ではありません。",
            "━" * 30,
            "",
        ]

        # LEVEL→FlowScore→名称で整列
        stocks_sorted = sorted(
            stocks,
            key=lambda s: (int(s.get("level", 0)), float(s.get("flow_score", 0))),
            reverse=True
        )

        for s in stocks_sorted[:30]:
            ticker = s.get("ticker", "")
            name_jp = TICKER_NAMES_JP.get(ticker, s.get("name", "")[:12])
            level = int(s.get("level", 0))
            flow = s.get("flow_score", 0)
            state = s.get("display_state", s.get("state", ""))
            tags = s.get("tags", [])
            tag_txt = " / ".join(tags[:4]) if tags else "-"
            lines.extend([
                f"LEVEL {level}  {ticker}（{name_jp}）",
                f"  FlowScore: {flow} / 状態: {state}",
                f"  タグ: {tag_txt}",
                "",
            ])

        msg.attach(MIMEText("\n".join(lines), "plain", "utf-8"))

        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(email, app_password)
            server.send_message(msg)
        return True
    except Exception:
        return False



# ==========================================
# カード表示
# ==========================================
def render_card(ticker: str, d: Dict, show_cap_badge: bool = False):
    """銘柄カードを表示（LEVEL + FlowScore + 状態タグ）"""
    flow_score = d.get("flow_score", 0)
    level = int(d.get("level", 0))
    ma_score = d.get("ma_score", None)
    state = d.get("display_state", d.get("state", "観測中"))
    tags = d.get("tags", [])

    # FlowScoreに基づくカードクラス（見た目の強弱）
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

    # 日本語名
    name_jp = TICKER_NAMES_JP.get(ticker, d.get('name', code))

    # タグ表示（最大4つ）
    tags_html = ""
    for tag in tags[:4]:
        # 要監視は目立たせる（グレー禁止）
        if tag == "要監視":
            tags_html += '<span style="background:#E8EAF6;color:#5C6BC0;padding:2px 8px;border-radius:999px;font-size:0.65rem;margin-right:6px;font-weight:700;">要監視</span>'
        else:
            tags_html += f'<span style="background:#F3F4F6;color:#444;padding:2px 8px;border-radius:999px;font-size:0.65rem;margin-right:6px;">{tag}</span>'

    # 表示用スコア（主はFlow。補助でLEVEL）
    score_text = f"{flow_score}"
    level_text = f"LEVEL {level}" if level > 0 else "LEVEL -"

    st.markdown(f"""
    <div class="spike-card {card_class}">
        <div class="card-header">
            <div class="ticker-name">
                <a href="{url}" target="_blank">{ticker}</a>
                <span style="font-size:0.75rem;color:#888;margin-left:6px;">{str(name_jp)[:12]}</span>
            </div>
            <div style="display:flex;align-items:center;gap:8px;">
                <span style="background:{level_color};color:white;padding:3px 10px;border-radius:12px;font-size:0.75rem;font-weight:700;">{level_text}</span>
                <div class="ratio-badge {score_class}">{score_text}</div>
            </div>
        </div>
        <div class="card-body">
            <div><span class="info-label">現在値</span><br><span class="info-value" style="color:#C41E3A;font-weight:600;">¥{d.get('price',0):,.0f}</span></div>
            <div><span class="info-label">状態</span><br><span class="info-value">{state}</span></div>
            <div><span class="info-label">時価総額</span><br><span class="info-value">{d.get('market_cap_oku',0):,}億円</span></div>
            <div><span class="info-label">出来高倍率</span><br><span class="info-value">{d.get('vol_ratio', 0)}x</span></div>
        </div>
        <div style="padding:0 0.8rem 0.5rem;font-size:0.7rem;">{tags_html}</div>
    </div>
    """, unsafe_allow_html=True)

    # チャートは画面遷移せず、その場で表示切替
    chart_open = st.session_state.setdefault("chart_open", {})
    is_open = bool(chart_open.get(ticker, False))

    toggle_label = "📊 チャートを閉じる" if is_open else "📊 チャートを表示"
    if st.button(toggle_label, key=f"chart_toggle_{ticker}", use_container_width=True):
        chart_open[ticker] = not is_open
        st.session_state["chart_open"] = chart_open

    if chart_open.get(ticker, False):
        # 期間選択（見た目はピル、実体はボタン）
        period_key = f"period_{ticker}"
        current_period = st.session_state.get(period_key, "6mo")
        periods = [("1ヶ月", "1mo"), ("3ヶ月", "3mo"), ("6ヶ月", "6mo"), ("1年", "1y")]

        st.markdown('<div class="pill-row">', unsafe_allow_html=True)
        cols = st.columns(len(periods))
        for i, (lab, val) in enumerate(periods):
            with cols[i]:
                btn_type = "primary" if current_period == val else "secondary"
                if st.button(lab, key=f"{period_key}_{val}", use_container_width=True, type=btn_type):
                    st.session_state[period_key] = val
        st.markdown('</div>', unsafe_allow_html=True)

        period = st.session_state.get(period_key, "6mo")

        with st.spinner("チャートを読み込み中..."):
            name = d.get("name", ticker)
            avg_volume = d.get("avg_volume", 0)
            flow_details = d.get("flow_details", {})
            fig = create_chart(ticker, name, period, avg_volume, flow_details)
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})

        st.caption("※表示は市場データの可視化です。銘柄推奨・売買助言ではありません。")


# ==========================================
# ログイン画面
# ==========================================
def show_login_page():
    """ログイン画面を表示"""
    logo_base64 = get_logo_base64()
    
    st.markdown("<div style='height: 60px;'></div>", unsafe_allow_html=True)
    
    # ログインコンテナ
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        # ロゴ表示
        if logo_base64:
            st.markdown(f"""
            <div style="text-align: center; margin-bottom: 1.5rem;">
                <img src="data:image/png;base64,{logo_base64}" style="max-width: 280px; width: 90%;">
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("<h1 style='text-align:center;'>🦅 源太AI ハゲタカSCOPE</h1>", unsafe_allow_html=True)
        
        # ログインフォーム
        st.markdown("""
        <div style="text-align: center; margin-bottom: 1rem;">
            <p style="color: #666; font-size: 0.9rem;">ログインしてください</p>
        </div>
        """, unsafe_allow_html=True)
        
        # エラーメッセージ表示
        if st.session_state.get("login_error"):
            st.markdown("""
            <div class="login-error">
                ❌ パスワードまたはメールアドレスが正しくありません
            </div>
            """, unsafe_allow_html=True)
        
        # パスワード/メールアドレス入力
        login_input = st.text_input(
            "パスワード / メールアドレス",
            type="default",
            placeholder="共通パスワード or 登録済みメールアドレス",
            key="login_input"
        )
        
        # ログインボタン
        if st.button("ログイン", use_container_width=True):
            # キャッシュをクリア（新しいセッションの開始）
            st.cache_data.clear()
            
            # 共通パスワードでログイン
            if login_input == MASTER_PASSWORD:
                st.session_state["logged_in"] = True
                st.session_state["login_error"] = False
                st.session_state["login_type"] = "master"
                st.session_state["email_address"] = ""
                st.session_state["app_password"] = ""
                st.rerun()
            else:
                # メールアドレスとしてスプレッドシートを検索
                try:
                    settings = load_settings_by_email(login_input)
                    if settings:
                        st.session_state["logged_in"] = True
                        st.session_state["login_error"] = False
                        st.session_state["login_type"] = "email"
                        st.session_state["email_address"] = settings["email"]
                        st.session_state["app_password"] = decrypt_password(settings["encrypted_password"])
                        st.rerun()
                    else:
                        st.session_state["login_error"] = True
                        st.rerun()
                except Exception as e:
                    # エラー時はキャッシュをクリアしてエラー表示
                    st.cache_data.clear()
                    st.session_state["login_error"] = True
                    st.rerun()
        
        # ヒント
        st.markdown("""
        <div style="background:#F5F5F5;border-radius:8px;padding:0.8rem;margin-top:1rem;font-size:0.75rem;color:#666;text-align:left;">
            <p style="margin:0 0 0.3rem 0;font-weight:bold;">💡 ログイン方法</p>
            <p style="margin:0 0 0.2rem 0;">• <strong>初回の方</strong>：共通パスワードを入力</p>
            <p style="margin:0;">• <strong>2回目以降</strong>：登録済みのメールアドレスを入力</p>
        </div>
        """, unsafe_allow_html=True)
        
        # フッター
        st.markdown("""
        <div style="text-align: center; margin-top: 2rem; color: #aaa; font-size: 0.75rem;">
            先乗り株カレッジ会員専用ツール
        </div>
        """, unsafe_allow_html=True)


# ==========================================
# メイン画面
# ==========================================
def show_main_page():
    """メインアプリ画面を表示"""
    
    # 初回同意チェック
    if not st.session_state.get("disclaimer_agreed"):
        show_disclaimer_page()
        return
    
    # チャート表示モードの場合
    if st.session_state.get("show_chart") and st.session_state.get("selected_ticker"):
        ticker = st.session_state["selected_ticker"]
        stock_info = st.session_state.get("selected_stock_info", {})
        show_chart_modal(ticker, stock_info)
        return
    
    logo_base64 = get_logo_base64()
    
    # ヘッダー表示
    if logo_base64:
        st.markdown(f"""
        <div style="text-align: center; margin-bottom: 0.5rem;">
            <img src="data:image/png;base64,{logo_base64}" style="max-width: 320px; width: 80%;">
        </div>
        """, unsafe_allow_html=True)
    else:
        st.title("🦅 HAGETAKA SCOPE")
    
    st.markdown(f'<p class="subtitle">M&A候補の早期検知ツール（時価総額{MARKET_CAP_MIN}億〜{MARKET_CAP_MAX}億円）</p>', unsafe_allow_html=True)
    
    # 免責表示（最小化可能）
    with st.expander("⚠️ ご利用にあたって", expanded=False):
        st.markdown(f"""
        <div style="font-size:0.8rem;color:#666;line-height:1.6;">
            {DISCLAIMER_TEXT}
        </div>
        """, unsafe_allow_html=True)
    
    # ログイン方法に応じたメッセージ
    if st.session_state.get("login_type") == "master":
        st.markdown("""
        <div class="hint-box">
            💡 <strong>ヒント</strong>：通知設定タブでメール設定を保存すると、次回からメールアドレスでログインでき、設定が自動で読み込まれます！
        </div>
        """, unsafe_allow_html=True)
    elif st.session_state.get("login_type") == "email":
        email = st.session_state.get("email_address", "")
        masked_email = email[:3] + "***" + email[email.find("@"):] if email and "@" in email else ""
        st.markdown(f"""
        <div class="success-box">
            🎉 <strong>設定を自動で読み込みました！</strong><br>
            <span style="font-size:0.8rem;">メール: {masked_email}</span>
        </div>
        """, unsafe_allow_html=True)
    
    # データ読み込み
    data = load_data()
    
    # タブ
    tab1, tab2 = st.tabs(["📊 M&A候補", "🔔 通知設定"])
    
    # ==========================================
    # タブ1: M&A候補
    # ==========================================
    with tab1:
        if data:
            updated_at = data.get("updated_at", "不明")
            level_counts = data.get("level_counts", {})
            
            st.markdown(f"""
            <div class="update-info">
                📡 最終更新: <strong>{updated_at}</strong><br>
                <span style="font-size:0.7rem;color:#666;">平日 16:30頃 に自動更新（土日祝は更新なし）</span>
            </div>
            """, unsafe_allow_html=True)
            
            
                        # LEVELガイド（邪魔しない表示）
            guide_md = """
**LEVELは“条件一致の多さ”を示す目安（1〜4）です。**
本ツールは市場データの可視化であり、銘柄推奨・売買助言ではありません。

- **LEVEL 1**：条件に触れた（変化は小さい）
- **LEVEL 2**：変化が見え始めた
- **LEVEL 3**：複数条件が重なっている
- **LEVEL 4**：条件一致が多く密度が高い

**要監視**：取引量が増えているのに値動きが小さい等、状態が“目立つ”ときに表示されます（理由は複数あります）。
"""

            # 画面を邪魔しないガイド（対応ブラウザはpopover、だめならexpander）
            try:
                with st.popover("❓LEVEL", use_container_width=False):
                    st.markdown(guide_md)
            except Exception:
                with st.expander("❓LEVELガイド", expanded=False):
                    st.markdown(guide_md)

            # 説明（中立表現）
            st.markdown("""
            <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                        border-radius: 10px; padding: 0.8rem; margin-bottom: 1rem; color: white; font-size: 0.8rem;">
                <strong>🎯 候補抽出の考え方</strong><br>
                「取引量が増えているのに値動きが小さい」などの条件一致を組み合わせ、候補を少数に絞って表示します。
            </div>
            """, unsafe_allow_html=True)

            # フィルター切替
            show_all = st.checkbox("全銘柄を表示（参考）", value=False)

            if show_all:
                display_data = data.get("all_data", {})
            else:
                display_data = data.get("data", {})

            # 統計（LEVEL別）
            lvl4 = len([v for v in display_data.values() if int(v.get("level", 0)) == 4])
            lvl3p = len([v for v in display_data.values() if int(v.get("level", 0)) >= 3])
            flow70 = len([v for v in display_data.values() if v.get("flow_score", 0) >= FLOW_SCORE_HIGH])

            col1, col2, col3 = st.columns(3)
            with col1:
                st.markdown(f'<div class="stat-box"><div class="stat-value high">{lvl4}</div><div class="stat-label">LEVEL 4</div></div>', unsafe_allow_html=True)
            with col2:
                st.markdown(f'<div class="stat-box"><div class="stat-value medium">{lvl3p}</div><div class="stat-label">LEVEL 3+</div></div>', unsafe_allow_html=True)
            with col3:
                st.markdown(f'<div class="stat-box"><div class="stat-value total">{len(display_data)}</div><div class="stat-label">表示件数</div></div>', unsafe_allow_html=True)

            st.markdown("")

            # LEVELフィルター（数字のみ / 見た目はプロっぽくボタン）
            level_options = [("ALL", "すべて"), ("4", "4"), ("3", "3"), ("2", "2"), ("1", "1")]
            selected_level = st.session_state.get("filter_level", "すべて")

            cols = st.columns(len(level_options))
            for i, (label, value) in enumerate(level_options):
                with cols[i]:
                    btn_type = "primary" if selected_level == value else "secondary"
                    if st.button(label, key=f"level_btn_{value}", use_container_width=True, type=btn_type):
                        st.session_state["filter_level"] = value
                        selected_level = value

            filter_level = selected_level

            if filter_level != "すべて":
                lv = int(filter_level)
                display_data = {k: v for k, v in display_data.items() if int(v.get("level", 0)) == lv}
            
            # カード表示
            if display_data:
                for ticker, d in sorted(display_data.items(), key=lambda x: (int(x[1].get('level',0)), float(x[1].get('ma_score',0)), float(x[1].get('flow_score',0))), reverse=True):
                    render_card(ticker, d)
            else:
                st.info("該当する銘柄がありません")
            
            # メール送信
            email = st.session_state.get("email_address", "")
            app_password = st.session_state.get("app_password", "")
            
            notify_stocks = [{"ticker": k, **v} for k, v in display_data.items() if v.get("flow_score", 0) >= FLOW_SCORE_MEDIUM]
            
            if notify_stocks and email and app_password:
                st.markdown("---")
                if st.button(f"📧 候補銘柄（{len(notify_stocks)}件）をメール送信"):
                    with st.spinner("送信中..."):
                        if send_spike_alert(email, app_password, notify_stocks, updated_at):
                            st.success(f"✅ 送信しました！")
                        else:
                            st.error("❌ 送信失敗。通知設定を確認してください。")
        else:
            st.markdown("""
            <div style="text-align:center;padding:2rem;color:#666;">
                <p style="font-size:2.5rem;">📊</p>
                <p>データがありません</p>
                <p style="font-size:0.8rem;color:#888;">GitHub Actionsで初回実行してください</p>
            </div>
            """, unsafe_allow_html=True)
    
    # ==========================================
    # タブ2: 通知設定
    # ==========================================
    with tab2:
        st.markdown("### 🔔 メール通知設定")
        st.markdown('<p style="color:#666;font-size:0.8rem;">出来高急動（1.5倍以上）を検知した際に通知を受け取れます</p>', unsafe_allow_html=True)
        
        # 現在のメールアドレス表示（ログイン済みの場合）
        current_email = st.session_state.get("email_address", "")
        if current_email:
            st.markdown(f"""
            <div style="background:#E8F5E9;border-radius:8px;padding:0.5rem 1rem;margin-bottom:1rem;font-size:0.85rem;">
                ✅ ログイン中: <strong>{current_email}</strong>
            </div>
            """, unsafe_allow_html=True)
        
        # メール設定入力
        email = st.text_input(
            "Gmailアドレス",
            value=st.session_state.get("email_address", ""),
            placeholder="example@gmail.com"
        )
        app_password = st.text_input(
            "アプリパスワード（16桁）",
            value=st.session_state.get("app_password", ""),
            type="password",
            placeholder="xxxx xxxx xxxx xxxx"
        )
        
        # ボタン行
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
                            st.session_state["email_address"] = email.lower().strip()
                            st.session_state["app_password"] = app_password
                            st.success(f"✅ 保存しました！次回から「{email}」でログインできます")
                        else:
                            st.error("❌ 保存に失敗しました")
        
        with col2:
            if st.button("🧪 テスト送信", use_container_width=True):
                if email and app_password:
                    with st.spinner("送信中..."):
                        ok, msg = send_test_email(email, app_password)
                        if ok:
                            st.success(f"✅ {msg}")
                        else:
                            st.error(f"❌ {msg}")
                else:
                    st.warning("⚠️ メールアドレスとパスワードを入力してください")
        
        with st.expander("📖 アプリパスワードの取得方法"):
            st.markdown("""
            1. [myaccount.google.com](https://myaccount.google.com/) にアクセス
            2. **セキュリティ** → **2段階認証** を有効化
            3. [アプリパスワード](https://myaccount.google.com/apppasswords) で生成
            4. 16桁のパスワードを上のフォームに入力
            
            ⚠️ 通常のGmailパスワードでは動作しません
            """)
        
        st.markdown("""
        <div style="background:#FFF5F5;border-radius:8px;padding:1rem;margin-top:1rem;font-size:0.8rem;color:#555;border:1px solid #FFE0E0;">
            <p style="font-weight:bold;color:#C41E3A;margin-bottom:0.5rem;">🔒 セキュリティについて</p>
            <ul style="margin:0;padding-left:1.2rem;color:#666;">
                <li>アプリパスワードは<strong>暗号化</strong>して保存されます</li>
                <li>次回からメールアドレスでログインできます</li>
                <li>どの端末・ブラウザからでも同じメールアドレスでログイン可能</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
        
        # ログアウトボタン
        st.markdown("---")
        if st.button("🚪 ログアウト", use_container_width=True):
            # キャッシュをクリア
            st.cache_data.clear()
            # セッション状態をリセット
            st.session_state["logged_in"] = False
            st.session_state["login_type"] = None
            st.session_state["email_address"] = ""
            st.session_state["app_password"] = ""
            st.session_state["login_error"] = False
            st.rerun()


# ==========================================
# 免責同意画面
# ==========================================
def show_disclaimer_page():
    """初回利用時の免責同意画面"""
    st.markdown("""
    <div style="text-align: center; margin: 2rem 0;">
        <h2>🦅 HAGETAKA SCOPE</h2>
        <p style="color: #666;">ご利用前に以下をご確認ください</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown(f"""
    <div style="background: #FFF3E0; border: 1px solid #FFB74D; border-radius: 10px; padding: 1.5rem; margin: 1rem 0;">
        <h4 style="color: #E65100; margin-top: 0;">⚠️ ご利用にあたっての注意事項</h4>
        <p style="font-size: 0.9rem; line-height: 1.8; color: #333;">
            {DISCLAIMER_TEXT}
        </p>
        <ul style="font-size: 0.85rem; color: #555; line-height: 1.8;">
            <li>本ツールは「市場データの可視化」を目的としています</li>
            <li>銘柄の推奨や売買の助言は一切行いません</li>
            <li>投資判断は必ずご自身の責任で行ってください</li>
            <li>表示される情報の正確性を保証するものではありません</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)
    
    # 同意チェックボックス
    agree1 = st.checkbox("本ツールは投資助言ではないことを理解しました")
    agree2 = st.checkbox("最終判断は自己責任で行うことを理解しました")
    
    if st.button("同意して利用開始", use_container_width=True, disabled=not (agree1 and agree2)):
        st.session_state["disclaimer_agreed"] = True
        st.rerun()


# ==========================================
# メイン処理
# ==========================================
# セッション初期化
if "logged_in" not in st.session_state:
    st.session_state["logged_in"] = False
if "login_error" not in st.session_state:
    st.session_state["login_error"] = False
if "show_chart" not in st.session_state:
    st.session_state["show_chart"] = False
if "selected_ticker" not in st.session_state:
    st.session_state["selected_ticker"] = None
if "chart_period" not in st.session_state:
    st.session_state["chart_period"] = "6mo"
if "disclaimer_agreed" not in st.session_state:
    st.session_state["disclaimer_agreed"] = False

# ページ表示
if st.session_state.get("logged_in"):
    show_main_page()
else:
    show_login_page()
