from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.telegram.hooks.telegram import TelegramHook

def send_telegram_success_notification(context):
    """
    Callback function that sends a success notification to Telegram using TelegramHook.
    
    Airflow automatically passes the `context` dictionary containing metadata 
    about the DAG run and Task Instance.
    """
    # 1. Initialize the Telegram Hook.
    # It retrieves the Bot Token and (optional) Chat ID from the Airflow Connection 'telegram_default'.
    # You can also pass 'chat_id' directly here if it's not configured in the connection.
    telegram_hook = TelegramHook(
        telegram_conn_id="telegram_default",
        # chat_id="YOUR_CHAT_ID"  # Optional: override or set here if not configured in the Connection
    )
    
    # 2. Extract run metadata from the context dictionary
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    
    # In Airflow 2.2+, context['logical_date'] is preferred over context['execution_date']
    execution_date = context.get('logical_date', context.get('execution_date'))
    
    # Construct a message (Markdown format)
    message = (
        f"✅ *Airflow Task Success Alert!*\n\n"
        f"• *DAG*: `{dag_id}`\n"
        f"• *Task*: `{task_id}`\n"
        f"• *Execution Date*: `{execution_date}`"
    )
    
    # 3. Send the message.
    # api_params parameters are passed directly to the python-telegram-bot send_message API.
    telegram_hook.send_message(
        api_params={
            # "chat_id": "YOUR_CHAT_ID",  # Optional: override/specify chat_id
            "text": message,
            "parse_mode": "Markdown",     # Enable Markdown formatting (e.g. *bold*, `code`)
        }
    )

# Define the DAG
with DAG(
    dag_id="test_telegram_callback",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    # Optional: Set at the DAG level to run when the entire DAG succeeds
    # on_success_callback=send_telegram_success_notification,
    tags=["telegram", "callback"],
) as dag:

    # Run the callback when this specific task succeeds
    task_1 = EmptyOperator(
        task_id="run_me",
        on_success_callback=send_telegram_success_notification,
    )
