import os
import csv
import json
from datetime import datetime

from airflow import DAG
from airflow.sdk import task

from airflow.providers.telegram.hooks.telegram import TelegramHook

def send_telegram_success_notification(context):
    """Callback triggered on DAG success. Formats a summary from XCom and posts to Telegram."""
    telegram_hook = TelegramHook(telegram_conn_id="telegram_default")

    ti = context['task_instance']
    dag_id = context['dag'].dag_id
    execution_date = context.get('logical_date', context.get('execution_date'))

    # Pull the spam report from XCom
    report_str = ti.xcom_pull(task_ids='scan_for_spam')
    summary = ""
    
    if report_str:
        try:
            evaluations = json.loads(report_str)
            total = len(evaluations)
            spam_count = sum(1 for e in evaluations if e.get("spam_score", 0) >= 80)
            summary = f"\n• *Total Reviews*: `{total}`\n• *Spam Detected*: `{spam_count}`"
        except Exception:
            pass

    message = (
        f"✅ *Airflow DAG Success Alert!*\n\n"
        f"• *DAG*: `{dag_id}`\n"
        f"• *Execution Date*: `{execution_date}`"
        f"{summary}"
    )

    telegram_hook.send_message(
        api_params={
            "text": message,
            "parse_mode": "Markdown",
        }
    )

def send_telegram_failure_notification(context):
    """Callback triggered on any task failure. Sends error details to Telegram."""
    telegram_hook = TelegramHook(telegram_conn_id="telegram_default")

    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context.get('logical_date', context.get('execution_date'))
    error = context.get('exception')

    message = (
        f"🚨 *Airflow Task Failure Alert!*\n\n"
        f"• *DAG*: `{dag_id}`\n"
        f"• *Task*: `{task_id}`\n"
        f"• *Execution Date*: `{execution_date}`"
    )
    if error:
        message += f"\n• *Error*: `{str(error)[:150]}`"

    telegram_hook.send_message(
        api_params={
            "text": message,
            "parse_mode": "Markdown",
        }
    )

with DAG(
    dag_id="Telegram_ai_spam_filter",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ai", "data-quality", "gemini"],
    on_success_callback=send_telegram_success_notification,
    on_failure_callback=send_telegram_failure_notification,
) as dag:


    @task
    def extract_reviews_from_csv() -> str:
        """อ่านข้อมูลรีวิวจากไฟล์ CSV ในเครื่องและแปลงเป็น JSON"""
        # ใน Cloud Composer โฟลเดอร์ 'dags' สามารถเข้าถึงได้จาก path นี้
        dag_folder = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(dag_folder, 'reviews.csv')
        
        reviews = []
        with open(csv_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                reviews.append({
                    "id": int(row["id"]),
                    "text": row["text"]
                })
                
        return json.dumps(reviews)

    # 2. ใช้ @task.llm (ส่งคืนค่าเป็น string ธรรมดา)
    @task.llm(
        model_id="google:gemini-3.1-flash-lite",
        llm_conn_id="google_ai",
    )
    def scan_for_spam(reviews_json: str):
        """ส่งข้อมูลดิบและคำสั่งไปให้ LLM ประมวลผล"""
        return f"""
        You are a Data Quality checker. Look at these product reviews.
        
        For each review, assign a 'spam_score' from 0 to 100:
        * 0 = Absolutely not spam
        * 100 = Absolute spam
        
        Also provide 1 sentence 'reason' in Thai language.
        
        Return a valid JSON array.
        Each object in the array must have exactly these 3 keys:
        - "review_id" (integer)
        - "spam_score" (integer)
        - "reason" (string)
        
        Reviews:
        {reviews_json}
        """

    @task
    def process_quality_report(report_str: str):
        """นำ JSON string ที่ได้จาก LLM มาประมวลผลต่อ"""
        import json
        
        # แปลง JSON string กลับเป็น list of dictionaries ใน Python
        evaluations = json.loads(report_str)
        
        print("====== รายงานผลสแปม ======")
        for eval in evaluations:
            review_id = eval.get("review_id")
            spam_score = eval.get("spam_score")
            reason = eval.get("reason")
            
            if spam_score >= 80:
                print(f"🚨 ไม่ผ่าน: รีวิวหมายเลข {review_id} ได้คะแนน Spam {spam_score}/100 (เหตุผล: {reason})")
            else:
                print(f"✅ ผ่าน: รีวิวหมายเลข {review_id} ได้คะแนน Spam {spam_score}/100 (เหตุผล: {reason})")
        print("===============================")

    # 3. กำหนด Workflow ของ DAG
    raw_data = extract_reviews_from_csv()
    qa_report = scan_for_spam(reviews_json=raw_data)
    process_quality_report(report_str=qa_report)