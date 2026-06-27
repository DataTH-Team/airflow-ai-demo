import os
import requests
from datetime import datetime

def load_env(env_path=None):
    path = env_path or os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(path):
        with open(path) as f:
            os.environ.update(
                (k.strip(), v.strip().strip("'\""))
                for line in f
                if "=" in line and not line.strip().startswith("#")
                for k, v in [line.split("=", 1)]
            )

def send_telegram_message(message: str):
    # Load env variables into os.environ
    load_env()
    
    token = os.getenv("TELEGRAM_TOKEN", "")
    # Read TELEGRAM_CHAT_ID (or fallback to LINE_TARGET_ID)
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    if message is None:
        message = f"hello {datetime.now()} test from Telegram"
        
    if not token:
        raise ValueError("TELEGRAM_TOKEN is not set in the environment or .env file.")
    if not chat_id:
        raise ValueError("TELEGRAM_CHAT_ID (or LINE_TARGET_ID) is not set.")
        
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message
    }
    
    response = requests.post(url, json=payload)
    response.raise_for_status()

if __name__ == "__main__":
    send_telegram_message(None)
