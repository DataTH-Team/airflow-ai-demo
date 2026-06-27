from datetime import datetime

from airflow import DAG
from airflow.sdk import task
from airflow.models.param import Param

with DAG(
    dag_id="LINE_ai_test_with_param",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ai", "gemini", "education"],
    params={
        "question": Param(
            "Apache Airflow คืออะไร สรุป 1 ประโยค",
            type="string",
            description="คำถามที่ต้องการส่งให้ Gemini",
        )
    },
) as dag:

    # ส่งคำถามให้ LLM
    @task.llm(
        model_id="google:gemini-3.1-flash-lite",
        llm_conn_id="google_ai",
    )
    def ask_gemini(question: str):
        return question

    @task
    def print_answer(answer: str):
        print("=" * 40)
        print(f"GEMINI ANSWER:\n{answer}")
        print("=" * 40)

    @task
    def send_to_line(answer: str):
        import requests
        import json
        from airflow.models import Variable

        # Retrieve credentials from Airflow Variables
        token = Variable.get("line_channels_access_token")
        target_id = Variable.get("line_target_id")

        # Use LINE Messaging API to push the answer
        url = 'https://api.line.me/v2/bot/message/push'
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}'
        }
        payload = {
            'to': target_id,
            'messages': [
                {
                    'type': 'text',
                    'text': answer
                }
            ]
        }
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        response.raise_for_status()

    # Trigger the tasks using DAG params
    response = ask_gemini(question="{{ params.question }}")
    print_answer(answer=response)
    send_to_line(answer=response)

