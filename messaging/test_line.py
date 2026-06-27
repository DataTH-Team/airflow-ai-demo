import os
import requests
import json
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


def send_line_messaging_api(message:str):
    # Load env variables into os.environ
    load_env()
    
    token = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
    target_id = os.getenv("LINE_TARGET_ID", "")
    
    if message is None:
        message = f"hello {datetime.now()} test"
    
    # Use LINE Messaging API instead.
    url = 'https://api.line.me/v2/bot/message/push'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    # Payload format dictated by the LINE Messaging API spec
    payload = {
        'to': target_id,
        'messages': [
            {
                'type': 'text',
                'text': message
            }
        ]
    }
    
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    response.raise_for_status()

if __name__ == "__main__":
    send_line_messaging_api(None)
