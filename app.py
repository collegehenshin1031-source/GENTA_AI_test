"""
HAGETAKA SCOPE - M&A候補検知ツール
- ログイン機能（共通パスワード or 登録済みメールアドレス）
- FlowScore（FlowScoreの強度）によるM&A候補検知
- 利用者ごとのメール通知機能（Google Sheets永続化）
- 診断カート機能（最大5件、戦略室連携用）

※本ツールは市場データの可視化を目的とした補助ツールです。
※銘柄推奨・売買助言ではありません。
"""

import json
import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, List, Optional
from pathlib import Path
import streamlit as st
from datetime import datetime
import pytz
import base64
import pandas as pd
import numpy as np

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
# 表記ゆれ吸収（フィルター/タグの安定化）
# ==========================================
STATE_HELP = {
    "要監視": "変化が強めです。優先して確認します。",
    "観測中": "変化の兆しがあります。数日単位で見守ります。",
    "沈静": "今は大きな変化が見えません。記録だけ残します。",
}

def _norm_label(s) -> str:
    """ラベルの表記ゆれを吸収（先頭記号/余計な空白/括弧など）"""
    if s is None:
        return ""
    t = str(s).strip()
    t = re.sub(r'^[\s○●◎◯・\-–—★☆▶▷→⇒✓✔✅☑︎【\[\(（]+', '', t).strip()
    return t

def _norm_tag(t) -> str:
    """タグの正規化"""
    t = _norm_label(t)
    if not t:
        return ""
    if "要監視" in t:
        return "要監視"
    return t

def _tags_list(x) -> List[str]:
    if x is None:
        return []
    if isinstance(x, list):
        return [str(v) for v in x]
    return [str(x)]

def _normalize_item(it: Dict) -> Dict:
    """表示/フィルター向けに状態とタグを安定化"""
    d = dict(it) if isinstance(it, dict) else {}

    raw_state = d.get("display_state", d.get("state", ""))
    tags_raw = _tags_list(d.get("tags"))
    tags_norm = []
    has_watch = ("要監視" in _norm_label(raw_state))
    
    for tg in tags_raw:
        nt = _norm_tag(tg)
        if not nt:
            continue
        if nt == "要監視":
            has_watch = True
            continue
        # 旧仕様のゾーンタグを除外
        if nt in ["下側ゾーン", "上側ゾーン"]:
            continue
        tags_norm.append(nt)

    state = _norm_label(raw_state) or "観測中"
    if has_watch:
        state = "要監視"
    d["display_state"] = state

    uniq = []
    seen = set()
    for t in tags_norm:
        if t not in seen:
            seen.add(t)
            uniq.append(t)
    d["tags"] = uniq
    return d

def _is_watch(item: dict) -> bool:
    """要監視判定"""
    state = _norm_label(item.get("display_state", item.get("state", "")))
    if "要監視" in state:
        return True
    for tg in _tags_list(item.get("tags")):
        if "要監視" in _norm_label(tg):
            return True
    return False

# ==========================================
# 日本語銘柄名辞書
# ==========================================
TICKER_NAMES_JP = {
    "3923.T": "ラクス", "4443.T": "Sansan", "4478.T": "フリー", "3994.T": "マネーフォワード",
    "4165.T": "プレイド", "4169.T": "ENECHANGE", "4449.T": "ギフティ", "4475.T": "HENNGE",
    "4431.T": "スマレジ", "4057.T": "インターファクトリー", "3697.T": "SHIFT", "4194.T": "ビジョナル",
    "4180.T": "Appier", "3655.T": "ブレインパッド", "4751.T": "サイバーエージェント",
    "3681.T": "ブイキューブ", "6035.T": "IRジャパン", "4384.T": "ラクスル", "9558.T": "ジャパニアス",
    "4441.T": "トビラシステムズ", "6315.T": "TOWA", "6323.T": "ローツェ", "6890.T": "フェローテック",
    "7735.T": "SCREENホールディングス", "6146.T": "ディスコ", "6266.T": "タツモ",
    "3132.T": "マクニカホールディングス", "6920.T": "レーザーテック", "4565.T": "そーせいグループ",
    "4587.T": "ペプチドリーム", "4582.T": "シンバイオ製薬", "4583.T": "カイオム・バイオ",
    "4563.T": "アンジェス", "2370.T": "メディネット", "4593.T": "ヘリオス", "3064.T": "MonotaRO",
    "3092.T": "ZOZO", "3769.T": "GMOペイメント", "4385.T": "メルカリ", "7342.T": "ウェルスナビ",
    "4480.T": "メドレー", "6560.T": "LTS", "3182.T": "オイシックス", "9166.T": "GENDA",
    "3765.T": "ガンホー", "3659.T": "ネクソン", "3656.T": "KLab", "3932.T": "アカツキ",
    "4071.T": "プラスアルファ", "4485.T": "JTOWER", "7095.T": "Macbee Planet",
    "4054.T": "日本情報クリエイト", "6095.T": "メドピア", "4436.T": "ミンカブ", "4477.T": "BASE",
}

# ==========================================
# UI設定・CSS
# ==========================================
st.set_page_config(page_title="源太AI🤖ハゲタカSCOPE", page_icon="🦅", layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800&display=swap');
#MainMenu, footer, header, .stDeployButton {display: none !important;}
html, body, [class*="css"]  { font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif !important; }
div[data-testid="stAppViewContainer"]{
  background: radial-gradient(1200px 600px at 10% 0%, rgba(92,107,192,0.10), transparent 60%),
              radial-gradient(900px 450px at 95% 10%, rgba(196,30,58,0.10), transparent 55%),
              linear-gradient(180deg, #FFFFFF 0%, #FAFBFF 60%, #FFFFFF 100%) !important;
}
.main .block-container{ max-width: 1080px !important; padding: 1.0rem 1.2rem 3.2rem 1.2rem !important; }
h1{ text-align:center !important; font-size: 1.55rem !important; font-weight: 800 !important; color: #0F172A !important; margin-bottom: .2rem !important; }
.subtitle{ text-align:center; color:#64748B; font-size:.85rem; margin-bottom: 1.1rem; }

/* Tabs */
.stTabs [data-baseweb="tab-list"]{
  justify-content:center !important; background: rgba(255,255,255,0.75) !important; backdrop-filter: blur(8px);
  padding: .35rem !important; border-radius: 14px !important; border: 1px solid rgba(15,23,42,0.08) !important;
  box-shadow: 0 10px 30px rgba(15,23,42,0.06) !important; margin-bottom: 1.0rem !important;
}
.stTabs [data-baseweb="tab"]{ padding: .55rem 1.05rem !important; border-radius: 10px !important; font-weight: 700 !important; color: #475569 !important; }
.stTabs [data-baseweb="tab"][aria-selected="true"]{ background: linear-gradient(135deg, #0F172A 0%, #334155 100%) !important; }
.stTabs [data-baseweb="tab"][aria-selected="true"] p{ color: #FFFFFF !important; }

/* Cards */
.spike-card{
  position: relative; background: rgba(255,255,255,0.92); backdrop-filter: blur(8px);
  border-radius: 16px; padding: 1rem; margin-bottom: .75rem; border: 1px solid rgba(15,23,42,0.12);
  box-shadow: 0 14px 44px rgba(15,23,42,0.10);
}
.spike-card::before{
  content:""; position:absolute; left:0; top:10px; bottom:10px; width:4px;
  border-radius: 999px; background: rgba(15,23,42,0.10);
}
.spike-card.high{ border-color: rgba(196,30,58,0.32); box-shadow: 0 18px 52px rgba(196,30,58,0.14); background: linear-gradient(180deg, rgba(255,255,255,0.95) 0%, rgba(255,245,247,0.92) 100%); }
.spike-card.high::before{ background: linear-gradient(180deg, #C41E3A 0%, #E63946 100%); }
.spike-card.medium{ border-color: rgba(255,152,0,0.32); box-shadow: 0 18px 52px rgba(255,152,0,0.12); background: linear-gradient(180deg, rgba(255,255,255,0.95) 0%, rgba(255,250,242,0.92) 100%); }
.spike-card.medium::before{ background: linear-gradient(180deg, #FF9800 0%, #FFC107 100%); }

.card-header{ display:flex; justify-content:space-between; align-items:center; gap:.7rem; margin-bottom: .55rem; }
.ticker-name a{ font-weight: 800; color:#0F172A !important; text-decoration:none; font-size: 1.1rem; }
.ticker-name a:hover{ text-decoration: underline; }

.ratio-badge{
  min-width: 70px; text-align:center; padding: .2rem .6rem; border-radius: 8px; font-weight: 800;
  border: 1px solid rgba(15,23,42,0.10); background: rgba(255,255,255,0.8); cursor: help;
}
.ratio-badge.high{ color:#FFFFFF; border-color: rgba(196,30,58,0.25); background: linear-gradient(135deg, #C41E3A 0%, #E63946 100%); }
.ratio-badge.medium{ color:#0F172A; border-color: rgba(255,152,0,0.25); background: linear-gradient(135deg, rgba(255,152,0,0.18) 0%, rgba(255,193,7,0.18) 100%); }
.score-val{ font-size: 1.0rem; line-height: 1.0; }
.score-label{ font-size: 0.55rem; line-height: 1.0; display: block; margin-bottom: 2px; opacity: 0.8;}

.card-body{ display:grid; grid-template-columns: repeat(4, 1fr); gap: .8rem; margin-top: .2rem; }
.info-label{ font-size: .72rem; color:#64748B; font-weight: 700; letter-spacing: .02em; }
.info-value{ font-size: .93rem; color:#0F172A; font-weight: 700; }

.stat-box{ background: rgba(255,255,255,0.88); border: 1px solid rgba(15,23,42,0.08); border-radius: 14px; padding: .9rem .9rem; box-shadow: 0 10px 30px rgba(15,23,42,0.06); text-align:center; }
.stat-value{ font-size: 1.55rem; font-weight: 900; line-height: 1.1; }
.stat-value.high{ color:#C41E3A; }
.stat-value.medium{ color:#FF9800; }
.stat-value.total{ color:#0F172A; }
.stat-label{ color:#64748B; font-size:.78rem; font-weight:700; margin-top:.25rem; }

div.stButton > button{ border-radius: 12px !important; font-weight: 800 !important; padding: .55rem .9rem !important; }

/* =======================================
   診断カートを右下に固定し、邪魔なアイコンを物理的に踏み潰す
   ======================================= */
div[data-testid="stPopover"]:has(p:contains("診断カート")) {
    position: fixed !important;
    bottom: 10px !important;
    right: 15px !important;
    z-index: 999999 !important; /* 常に一番手前に表示 */
    background: #ffffff !important;
    border-radius: 12px !important;
    box-shadow: 0px 5px 25px rgba(0,0,0,0.4) !important;
}

/* 覆い隠す面積を広げるため、ボタン自体を少し大きく太くする */
div[data-testid="stPopover"]:has(p:contains("診断カート")) > button {
    padding: 0.8rem 1.8rem !important;
    border: 2px solid #0F172A !important;
    background: #ffffff !important;
}


</style>
""", unsafe_allow_html=True)

# ==========================================
# ヘルパー関数
# ==========================================
def get_logo_base64():
    try:
        with open("logo.png", "rb") as f:
            return base64.b64encode(f.read()).decode()
    except:
        return None

@st.cache_data(ttl=60)
def load_data() -> Dict:
    data_path = Path("data/ratios.json")
    if data_path.exists():
        with open(data_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def get_fernet() -> Fernet:
    return Fernet(st.secrets["encryption"]["key"].encode())

def encrypt_password(password: str) -> str:
    if not password: return ""
    return get_fernet().encrypt(password.encode()).decode()

def decrypt_password(encrypted_password: str) -> str:
    if not encrypted_password: return ""
    try:
        return get_fernet().decrypt(encrypted_password.encode()).decode()
    except:
        return ""

def get_gsheets_connection():
    return st.connection("gsheets", type=GSheetsConnection)

def load_settings_by_email(email: str) -> Optional[Dict]:
    if not email: return None
    try:
        conn = get_gsheets_connection()
        df = conn.read(worksheet="settings", usecols=[0, 1], ttl=0)
        if df is None or df.empty: return None
        df.columns = ["email", "encrypted_password"]
        row = df[df["email"].str.lower().str.strip() == email.lower().strip()]
        if row.empty: return None
        return {"email": row.iloc[0]["email"], "encrypted_password": row.iloc[0]["encrypted_password"]}
    except:
        st.cache_data.clear()
        return None

def get_gspread_client():
    try:
        creds_dict = dict(st.secrets["connections"]["gsheets"])
        creds_dict.pop("spreadsheet", None)
        creds_dict.pop("worksheet", None)
        creds = Credentials.from_service_account_info(creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        return gspread.authorize(creds)
    except:
        return None

def save_settings_to_sheet(email: str, app_password: str) -> bool:
    if not email: return False
    email = email.lower().strip()
    try:
        client = get_gspread_client()
        if not client: return False
        url = st.secrets["connections"]["gsheets"].get("spreadsheet")
        worksheet = client.open_by_url(url).worksheet("settings")
        enc_pw = encrypt_password(app_password)
        try: all_emails = worksheet.col_values(1)
        except: all_emails = []
        
        row_index = -1
        for i, ce in enumerate(all_emails):
            if ce and ce.lower().strip() == email:
                row_index = i + 1
                break
                
        if row_index > 1:
            worksheet.update_cell(row_index, 2, enc_pw)
        else:
            worksheet.append_row([email, enc_pw])
        st.cache_data.clear()
        return True
    except:
        return False

def send_test_email(email: str, app_password: str) -> tuple[bool, str]:
    try:
        msg = MIMEMultipart()
        msg["From"] = msg["To"] = email
        msg["Subject"] = "🦅 ハゲタカSCOPE - テスト通知"
        msg.attach(MIMEText("メール設定が正常に完了しました！", "plain", "utf-8"))
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(email, app_password)
            server.send_message(msg)
        return True, "テストメール送信成功！"
    except Exception as e:
        return False, f"送信エラー: {str(e)}"

def format_volume_pct(v) -> str:
    if v is None: return "-"
    try:
        fv = float(v)
        if not np.isfinite(fv): return "-"
        return "<0.01%" if fv < 0.01 else f"{fv:.2f}%"
    except:
        return "-"

# ==========================================
# カード表示
# ==========================================
def render_card(ticker: str, d: Dict):
    flow_score = d.get("flow_score", 0)
    level = int(d.get("level", 0))
    state = d.get("display_state", d.get("state", "観測中"))
    
    _state_clean = _norm_label(state) or str(state).strip()
    _tip = STATE_HELP.get(_state_clean, "状態の目安です。").replace('"', "&quot;")
    if _state_clean == "要監視":
        state_html = f'<span title="{_tip}" style="color:#5C6BC0;font-weight:800;">{_state_clean}</span>'
    else:
        state_html = f'<span title="{_tip}">{_state_clean}</span>'

    tags = d.get("tags", [])
    if flow_score >= FLOW_SCORE_HIGH:
        card_class, score_class = "high", "high"
    elif flow_score >= FLOW_SCORE_MEDIUM:
        card_class, score_class = "medium", "medium"
    else:
        card_class, score_class = "", "normal"

    level_color = LEVEL_COLORS.get(level, "#9E9E9E")
    code = ticker.replace(".T", "")
    url = f"https://finance.yahoo.co.jp/quote/{code}.T"
    name_jp = TICKER_NAMES_JP.get(ticker, d.get('name', code))

    tags_html = ""
    for tag in tags[:4]:
        if tag == "要監視":
            tags_html += '<span style="background:#E8EAF6;color:#5C6BC0;padding:2px 8px;border-radius:999px;font-size:0.65rem;margin-right:6px;font-weight:700;">要監視</span>'
        else:
            tags_html += f'<span style="background:#F3F4F6;color:#444;padding:2px 8px;border-radius:999px;font-size:0.65rem;margin-right:6px;">{tag}</span>'

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
                <div class="ratio-badge {score_class}" title="※需給変化の強さを示す独自スコア（売買助言ではありません）">
                    <span class="score-label">フロースコア</span>
                    <span class="score-val">{score_text}</span>
                </div>
            </div>
        </div>
        <div class="card-body">
            <div><span class="info-label">現在値</span><br><span class="info-value" style="color:#C41E3A;font-weight:600;">¥{d.get('price',0):,.0f}</span></div>
            <div><span class="info-label">状態</span><br><span class="info-value">{state_html}</span></div>
            <div><span class="info-label">時価総額</span><br><span class="info-value">{d.get('market_cap_oku',0):,}億円</span></div>
            <div>
                <span class="info-label">出来高</span><br>
                <span class="info-value">{d.get('vol_ratio', 0)}x</span>
                <div style="margin-top:2px;font-size:0.72rem;color:#64748B;font-weight:700;">
                    株数比 {format_volume_pct(d.get('volume_of_shares_pct'))}{'（推定）' if d.get('volume_of_shares_pct_is_estimated') else ''}
                </div>
            </div>
        </div>
        <div style="padding:0 0.8rem 0.5rem;font-size:0.7rem;">{tags_html}</div>
    </div>
    """, unsafe_allow_html=True)

    # カート追加ボタン
    if ticker in st.session_state["cart"]:
        if st.button("🛒 カートから外す", key=f"cart_{ticker}", use_container_width=True):
            st.session_state["cart"].remove(ticker)
            st.rerun()
    else:
        is_full = len(st.session_state["cart"]) >= 5
        if st.button("🛒 カートに追加", key=f"cart_{ticker}", use_container_width=True, disabled=is_full):
            if not is_full:
                st.session_state["cart"].append(ticker)
                st.rerun()

# ==========================================
# 画面遷移
# ==========================================
def show_login_page():
    logo_base64 = get_logo_base64()
    st.markdown("<div style='height: 60px;'></div>", unsafe_allow_html=True)
    _, col2, _ = st.columns([1, 2, 1])
    with col2:
        if logo_base64:
            st.markdown(f'<div style="text-align: center; margin-bottom: 1.5rem;"><img src="data:image/png;base64,{logo_base64}" style="max-width: 280px; width: 90%;"></div>', unsafe_allow_html=True)
        else:
            st.markdown("<h1 style='text-align:center;'>🦅 源太AI ハゲタカSCOPE</h1>", unsafe_allow_html=True)
        
        if st.session_state.get("login_error"):
            st.error("❌ パスワードまたはメールアドレスが正しくありません")
        
        login_input = st.text_input("パスワード / メールアドレス", placeholder="共通パスワード or 登録済みメール", key="login_input")
        if st.button("ログイン", use_container_width=True, type="primary"):
            st.cache_data.clear()
            if login_input == MASTER_PASSWORD:
                st.session_state.update({"logged_in": True, "login_error": False, "login_type": "master", "email_address": "", "app_password": ""})
                st.rerun()
            else:
                try:
                    settings = load_settings_by_email(login_input)
                    if settings:
                        st.session_state.update({"logged_in": True, "login_error": False, "login_type": "email", "email_address": settings["email"], "app_password": decrypt_password(settings["encrypted_password"])})
                        st.rerun()
                    else:
                        st.session_state["login_error"] = True
                        st.rerun()
                except:
                    st.cache_data.clear()
                    st.session_state["login_error"] = True
                    st.rerun()

def show_disclaimer_page():
    st.markdown("<div style='text-align: center; margin: 2rem 0;'><h2>🦅 HAGETAKA SCOPE</h2></div>", unsafe_allow_html=True)
    st.warning(DISCLAIMER_TEXT)
    if st.button("同意して利用開始", use_container_width=True, disabled=not (st.checkbox("本ツールは投資助言ではないことを理解しました") and st.checkbox("最終判断は自己責任で行うことを理解しました"))):
        st.session_state["disclaimer_agreed"] = True
        st.rerun()

def show_main_page():
    if not st.session_state.get("disclaimer_agreed"):
        show_disclaimer_page()
        return
    
    logo_base64 = get_logo_base64()
    if logo_base64:
        st.markdown(f'<div style="text-align: center; margin-bottom: 0.5rem;"><img src="data:image/png;base64,{logo_base64}" style="max-width: 320px; width: 80%;"></div>', unsafe_allow_html=True)
    else:
        st.title("🦅 HAGETAKA SCOPE")
    st.markdown(f'<p class="subtitle">M&A候補の早期検知ツール（時価総額{MARKET_CAP_MIN}億〜{MARKET_CAP_MAX}億円）</p>', unsafe_allow_html=True)
    
    data = load_data()
    tab1, tab2 = st.tabs(["📊 M&A候補", "🔔 通知設定"])
    
    with tab1:
        if data:
            updated_at = data.get("updated_at", "不明")
            st.caption(f"📡 最終更新: {updated_at}")
            
            show_all = st.checkbox("中型株以外も表示", value=False)
            display_data = data.get("all_data", {}) if show_all else data.get("data", {})
            display_data = {tk: _normalize_item(it) for tk, it in (display_data or {}).items()}

            lvl4 = len([v for v in display_data.values() if int(v.get("level", 0)) == 4])
            lvl3p = len([v for v in display_data.values() if int(v.get("level", 0)) >= 3])
            col1, col2, col3 = st.columns(3)
            with col1: st.markdown(f'<div class="stat-box"><div class="stat-value high">{lvl4}</div><div class="stat-label">LEVEL 4</div></div>', unsafe_allow_html=True)
            with col2: st.markdown(f'<div class="stat-box"><div class="stat-value medium">{lvl3p}</div><div class="stat-label">LEVEL 3+</div></div>', unsafe_allow_html=True)
            with col3: st.markdown(f'<div class="stat-box"><div class="stat-value total">{len(display_data)}</div><div class="stat-label">表示件数</div></div>', unsafe_allow_html=True)
            st.markdown("<br>", unsafe_allow_html=True)

            # ツールバー（フィルター ＆ カート）
            tb1, tb2, tb3 = st.columns([1.5, 1.5, 2.0])
            with tb1:
                try:
                    with st.popover("🔎 フィルター"):
                        st.session_state["flt_query"] = st.text_input("検索", value=st.session_state.get("flt_query", ""))
                        st.session_state["flt_levels"] = st.multiselect("LEVEL", options=["4", "3", "2", "1"], default=st.session_state.get("flt_levels") or [])
                        st.session_state["flt_watch_only"] = st.toggle("要監視のみ", value=bool(st.session_state.get("flt_watch_only")))
                        if st.button("🔄 リセット", use_container_width=True):
                            st.session_state.update({"flt_levels": [], "flt_watch_only": False, "flt_query": ""})
                            st.rerun()
                except:
                    pass

            with tb2:
                cart = st.session_state.get("cart", [])
                try:
                    with st.popover(f"🛒 診断カート ({len(cart)}/5)"):
                        if not cart:
                            st.write("カートは空です")
                        else:
                            for c in cart: st.write(f"・ {c} （{TICKER_NAMES_JP.get(c, '')}）")
                            if st.button("🗑 全削除", use_container_width=True):
                                st.session_state["cart"] = []
                                st.rerun()
                            st.markdown("---")
                            st.caption("※後日、戦略室への連携機能が追加されます")
                            st.code(",".join(cart), language="text")
                except:
                    pass

            with tb3:
                chips = []
                if st.session_state.get("flt_levels"): chips.append("LEVEL: " + ",".join(st.session_state["flt_levels"]))
                if st.session_state.get("flt_watch_only"): chips.append("要監視のみ")
                if chips: st.caption("適用中: " + " / ".join(chips))

            # フィルター適用
            q = (st.session_state.get("flt_query") or "").strip().lower()
            levels = st.session_state.get("flt_levels") or []
            w_only = bool(st.session_state.get("flt_watch_only"))
            
            filtered_data = {}
            for tk, it in display_data.items():
                if levels and str(int(it.get("level", 0))) not in levels: continue
                if w_only and not _is_watch(it): continue
                if q and q not in f"{tk} {(it.get('name') or '')}".lower(): continue
                filtered_data[tk] = it

            if filtered_data:
                for ticker, d in sorted(filtered_data.items(), key=lambda x: (int(x[1].get('level',0)), float(x[1].get('flow_score',0))), reverse=True):
                    render_card(ticker, d)
            else:
                st.info("該当する銘柄がありません")
        else:
            st.info("データがありません。GitHub Actionsを実行してください。")

    with tab2:
        st.markdown("### 🔔 メール通知設定 (テスト環境では送信されません)")
        st.info("※テスト環境のため、実際のメールは送信されません。")
        if st.button("🚪 ログアウト", type="primary"):
            st.cache_data.clear()
            st.session_state.update({"logged_in": False, "login_type": None, "email_address": "", "app_password": "", "cart": []})
            st.rerun()

# ==========================================
# メイン処理
# ==========================================
if "logged_in" not in st.session_state: st.session_state["logged_in"] = False
if "cart" not in st.session_state: st.session_state["cart"] = []
if st.session_state.get("logged_in"): show_main_page()
else: show_login_page()
