from datetime import datetime
from airflow import DAG
from airflow.sdk import task
from airflow.models.param import Param

def send_to_line(message: str):
    """Local helper function to send messages to LINE using environment variables."""
    import os
    import requests
    import json

    token = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
    target_id = os.getenv("LINE_TARGET_ID")
    if not token or not target_id:
        return

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
                'text': message
            }
        ]
    }
    requests.post(url, headers=headers, data=json.dumps(payload)).raise_for_status()

def send_line_success(context):
    """Callback triggered when the DAG succeeds. Sends the question and answer to LINE."""
    ti = context['task_instance']
    gemini_answer = ti.xcom_pull(task_ids='ask_gemini')
    dag_id = context['dag'].dag_id
    question = context.get('params', {}).get('question', 'Unknown')

    message_text = f"✅ Airflow DAG '{dag_id}' Succeeded!\n\n❓ Question: {question}\n\n🤖 Gemini Answer:\n{gemini_answer}"
    send_to_line(message_text)

def send_line_failure(context):
    """Callback triggered if any task fails. Sends failure report to LINE."""
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    error = context.get('exception')

    msg = f"🚨 Airflow Task Failed!\n• DAG: {dag_id}\n• Task: {task_id}"
    if error:
        msg += f"\n• Error: {str(error)[:200]}"
    send_to_line(msg)

with DAG(
    dag_id="LINE_simple_ai_with_callback",
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
    on_success_callback=send_line_success,
    on_failure_callback=send_line_failure,
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

    # Trigger the tasks using DAG params
    response = ask_gemini(question="{{ params.question }}")
    print_answer(answer=response)
