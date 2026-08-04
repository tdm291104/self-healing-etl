import requests
from agent.config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID


def send_message(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[Telegram disabled]\n{text}")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    resp = requests.post(url, json={
        'chat_id': TELEGRAM_CHAT_ID,
        'text': text,
        'parse_mode': 'Markdown',
    }, timeout=10)
    return resp.ok


def format_agent_message(date: str, decision: str, diagnosis: str,
                          action: str, confidence: float) -> str:
    icon = {'auto_fix': '✅', 'needs_approval': '⚠️', 'escalate': '🚨'}.get(decision, '❓')
    return (
        f"{icon} *Self-Healing ETL — {date}*\n\n"
        f"*Decision:* `{decision}`\n"
        f"*Confidence:* {confidence:.0%}\n\n"
        f"*Diagnosis:*\n{diagnosis}\n\n"
        f"*Action:*\n{action}"
    )
