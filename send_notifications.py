"""
源太AI🤖ハゲタカSCOPE - 自動メール通知スクリプト
GitHub Actionsで毎日自動実行され、登録ユーザー全員にメール通知を送信
"""

import json
import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from datetime import datetime
import pytz

# Google Sheets API
import gspread
from google.oauth2.service_account import Credentials

# 暗号化
from cryptography.fernet import Fernet

# ==========================================
# 定数
# ==========================================
JST = pytz.timezone("Asia/Tokyo")
FLOW_NOTIFY_THRESHOLD = 40  # FlowScoreがこの値以上の銘柄を通知対象

# 日本語銘柄名辞書
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
# 暗号化キー取得・復号化
# ==========================================
def get_encryption_key() -> str:
    """環境変数から暗号化キーを取得"""
    key = os.environ.get("ENCRYPTION_KEY")
    if not key:
        raise ValueError("ENCRYPTION_KEY environment variable is not set")
    return key


def decrypt_password(encrypted_password: str) -> str:
    """暗号化されたパスワードを復号化"""
    if not encrypted_password:
        return ""
    try:
        key = get_encryption_key()
        fernet = Fernet(key.encode())
        decrypted = fernet.decrypt(encrypted_password.encode())
        return decrypted.decode()
    except Exception as e:
        print(f"復号化エラー: {e}")
        return ""


# ==========================================
# Google Sheets接続
# ==========================================
def get_gspread_client():
    """Google Sheets APIクライアントを取得"""
    # 環境変数からサービスアカウント情報を取得
    credentials_json = os.environ.get("GSHEETS_CREDENTIALS")
    if not credentials_json:
        raise ValueError("GSHEETS_CREDENTIALS environment variable is not set")
    
    credentials_dict = json.loads(credentials_json)
    
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    
    credentials = Credentials.from_service_account_info(credentials_dict, scopes=scopes)
    client = gspread.authorize(credentials)
    
    return client


def load_all_users() -> list:
    """スプレッドシートから全ユーザーの設定を読み込み"""
    try:
        client = get_gspread_client()
        spreadsheet_url = os.environ.get("SPREADSHEET_URL")
        if not spreadsheet_url:
            raise ValueError("SPREADSHEET_URL environment variable is not set")
        
        spreadsheet = client.open_by_url(spreadsheet_url)
        worksheet = spreadsheet.worksheet("settings")
        
        # 全データを取得
        records = worksheet.get_all_records()
        
        users = []
        for record in records:
            email = record.get("email", "")
            encrypted_password = record.get("encrypted_password", "")
            
            if email and encrypted_password:
                password = decrypt_password(encrypted_password)
                if password:
                    users.append({
                        "email": email,
                        "app_password": password
                    })
        
        return users
    
    except Exception as e:
        print(f"ユーザー読み込みエラー: {e}")
        return []


# ==========================================
# データ読み込み
# ==========================================
def load_spike_data() -> dict:
    """出来高急動データを読み込み"""
    data_path = Path("data/ratios.json")
    if data_path.exists():
        with open(data_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


# ==========================================
# メール送信
# ==========================================
def create_email_body(data: dict) -> tuple[str, str]:
    """メール本文を作成（件名, 本文）"""
    updated_at = data.get("updated_at", "不明")
    stocks_data = data.get("data", {})
    
    # FlowScoreが一定以上の銘柄を抽出（ハゲタカSCOPEの候補通知）
    notify_stocks = {k: v for k, v in stocks_data.items() if float(v.get("flow_score", 0) or 0) >= FLOW_NOTIFY_THRESHOLD}
    if not notify_stocks:
        return None, None  # 通知する銘柄がない
    
    # 件名（数字と日付だけで分かるように）
    subject = f"🦅 ハゲタカSCOPE 要監視候補: {len(notify_stocks)}件 - {updated_at[:10]}"
    
    # 本文作成
    lines = [
        "━" * 40,
        "🦅 源太AI ハゲタカSCOPE（自動通知）",
        "━" * 40,
        f"更新日時: {updated_at}",
        f"要監視候補（FlowScore {FLOW_NOTIFY_THRESHOLD}+）: {len(notify_stocks)}件",
        "",
        "※本通知は市場データの可視化（条件一致）です。銘柄推奨・売買助言ではありません。",
        "━" * 40,
    ]
    
    # FlowScoreの高い順で上位を並べる
    for ticker, d in sorted(notify_stocks.items(), key=lambda x: float(x[1].get("flow_score", 0) or 0), reverse=True):
        name_jp = TICKER_NAMES_JP.get(ticker, d.get("name", "")[:12])
        level = d.get("level", 0)
        state = d.get("state", "通常")
        lines.extend([
            f"・{ticker} ({name_jp})",
            f"   LEVEL: {level} | 状態: {state} | FlowScore: {d.get('flow_score', 0)}",
            f"   現在値: ¥{d.get('price', 0):,.0f} | 時価総額: {d.get('market_cap_oku', 0)}億円",
            "",
        ])
    
    lines.extend([
        "━" * 40,
        "📱 詳細はアプリで確認",
        "https://hagetaka-scope-test.streamlit.app/",
        "━" * 40,
        "",
        "※ このメールは自動送信されています",
        "※ 配信停止はアプリの通知設定から行えます",
    ])
    
    body = "\n".join(lines)
    return subject, body


def send_email(email: str, app_password: str, subject: str, body: str) -> bool:
    """メールを送信"""
    try:
        msg = MIMEMultipart()
        msg["From"] = email
        msg["To"] = email
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain", "utf-8"))
        
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(email, app_password)
            server.send_message(msg)
        
        return True
    except Exception as e:
        print(f"メール送信エラー ({email}): {e}")
        return False


# ==========================================
# メイン処理
# ==========================================
def main():
    print("=" * 50)
    print("📧 自動メール通知スクリプト開始")
    print("=" * 50)
    
    # 出来高データ読み込み
    print("\n📊 出来高データを読み込み中...")
    data = load_spike_data()
    
    if not data:
        print("❌ 出来高データが見つかりません")
        return
    
    print(f"✅ データ読み込み完了: {data.get('updated_at', '不明')}")
    
    # メール本文作成
    subject, body = create_email_body(data)
    
    if not subject:
        print("📭 通知対象の銘柄がありません（1.5倍以上なし）")
        print("✅ 正常終了（通知スキップ）")
        return
    
    print(f"📝 件名: {subject}")
    
    # ユーザー一覧取得
    print("\n👥 ユーザー一覧を取得中...")
    users = load_all_users()
    
    if not users:
        print("❌ 通知対象のユーザーがいません")
        return
    
    print(f"✅ {len(users)}人のユーザーを取得")
    
    # 各ユーザーにメール送信
    print("\n📧 メール送信開始...")
    success_count = 0
    fail_count = 0
    
    for user in users:
        email = user["email"]
        app_password = user["app_password"]
        
        if send_email(email, app_password, subject, body):
            print(f"  ✅ {email[:3]}***{email[email.find('@'):]}")
            success_count += 1
        else:
            print(f"  ❌ {email[:3]}***{email[email.find('@'):]} (送信失敗)")
            fail_count += 1
    
    # 結果サマリー
    print("\n" + "=" * 50)
    print("📊 送信結果")
    print("=" * 50)
    print(f"✅ 成功: {success_count}件")
    print(f"❌ 失敗: {fail_count}件")
    print(f"📧 合計: {len(users)}件")
    print("=" * 50)
    print("✅ 自動メール通知スクリプト完了")


if __name__ == "__main__":
    main()
