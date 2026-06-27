# ตัวอย่าง Airflow AI DAGs

โปรเจกต์นี้ประกอบด้วยตัวอย่าง Apache Airflow DAGs ที่แสดงวิธีการเชื่อมต่อกับโมเดล AI ของ Google (Gemini) ผ่านการใช้งาน `@task.llm` decorator

## ไฟล์ที่มีอยู่ในโฟลเดอร์ `dags/`

- **`dags/dag1_simple_ai.py` (DAG: `ai_test`)**: ตัวอย่างการทำงานพื้นฐานที่แสดงการส่งคำถามสั้นๆ ไปยัง AI (เช่น ถามว่า "Apache Airflow คืออะไร สรุป 1 ประโยค") และพิมพ์คำตอบที่ได้ออกมา

  ```mermaid
  graph LR
      %% Define styles for the boxes
      classDef ai fill:#E3F2FD,stroke:#2196F3,stroke-width:2px,color:#0D47A1;
      classDef process fill:#FFF3E0,stroke:#FF9800,stroke-width:2px,color:#E65100;

      %% Define the nodes (functions)
      A[ask_gemini]
      B[print_answer]

      %% Define the flow
      A -->|"String (Response)"| B

      %% Apply styles
      class A ai;
      class B process;
  ```

- **`dags/dag2_ai_spam_filter.py` (DAG: `ai_spam_filter`)**: เวิร์กโฟลว์ขั้นสูงที่ทำงานเป็นตัวตรวจสอบคุณภาพข้อมูล (Data Quality checker) โดยโปรแกรมจะอ่านข้อมูลรีวิวจากไฟล์ CSV จากนั้นส่งให้ AI ช่วยประเมินความเป็นสแปมของรีวิวแต่ละอัน และรับคำตอบกลับมาในรูปแบบ JSON (ประกอบด้วยคะแนนสแปมและเหตุผลภาษาไทย)

  ```mermaid
  graph LR
      classDef extract fill:#E8F5E9,stroke:#4CAF50,stroke-width:2px,color:#1B5E20;
      classDef ai fill:#E3F2FD,stroke:#2196F3,stroke-width:2px,color:#0D47A1;
      classDef process fill:#FFF3E0,stroke:#FF9800,stroke-width:2px,color:#E65100;

      A[extract_reviews_from_csv]
      B[scan_for_spam]
      C[process_quality_report]

      A -->|"JSON (Raw)"| B
      B -->|"JSON (Scores)"| C

      class A extract;
      class B ai;
      class C process;
  ```

- **`dags/reviews.csv`**: ไฟล์ข้อมูลรีวิวตัวอย่างที่ใช้ใน DAG `ai_spam_filter`

---

## การตั้งค่าและใช้งานการแจ้งเตือน (Messaging & Callbacks)

โปรเจกต์นี้สาธิตเวิร์กโฟลว์การส่งการแจ้งเตือนผ่าน **LINE** และ **Telegram** ทั้งในรูปแบบ Task ปกติและในรูปแบบ Callback เมื่อ DAG ทำงานสำเร็จหรือล้มเหลว:

- **`dags/dag3_simple_ai_with_param_and_line.py` (DAG: `LINE_simple_ai`)**: 
  เรียกใช้งาน LINE Messaging API ผ่าน HTTP POST Request เป็น Task หนึ่งในเวิร์กโฟลว์ เพื่อส่งคำถามของฝั่งผู้ใช้และคำตอบของ Gemini เข้าห้องแชท LINE

- **`dags/dag4_simple_ai_with_param_callback.py` (DAG: `LINE_simple_ai_with_callback`)**: 
  ใช้ `on_success_callback` และ `on_failure_callback` ในการดึงผลลัพธ์คำตอบจาก XCom และส่งข้อมูลแจ้งเตือนสถานะความสำเร็จ/ความล้มเหลวของการรัน Pipeline ไปยัง LINE (ไม่ต้องสร้าง Task ส่งข้อมูลแยกต่างหาก)

- **`dags/dag5_ai_spam_filter_telegram.py` (DAG: `Telegram_ai_spam_filter`)**: 
  เรียกใช้ `on_success_callback` และ `on_failure_callback` ผ่าน **`TelegramHook`** ของ Airflow เพื่อสรุปผลลัพธ์การประเมินสแปม (จาก XCom) และส่งข้อมูลสถานะการรัน Pipeline ไปยัง Telegram

### การเตรียมบัญชีและข้อมูลสำหรับ LINE และ Telegram

#### 1. LINE Messaging API
- สมัครและสร้าง **LINE Official Account (Provider & Channel)** โดยปฏิบัติตามคำแนะนำอย่างละเอียดที่ [LINE Developers: Getting started with the Messaging API](https://developers.line.biz/en/docs/messaging-api/getting-started/)
- เมื่อตั้งค่าเสร็จสิ้น ให้เปิดใช้งานแชท และคัดลอก **Channel Access Token** มาใช้เป็น `LINE_CHANNEL_ACCESS_TOKEN`
- ใช้ ID ห้องแชทของผู้ใช้/กลุ่มที่ระบุในแผงควบคุม LINE Developers มาใช้เป็น `LINE_TARGET_ID`

#### 2. Telegram Bot
- คุยกับ [@BotFather](https://t.me/BotFather) ในแอปพลิเคชัน Telegram เพื่อสร้างบอทใหม่ด้วยการส่งคำสั่ง `/newbot` จากนั้นคัดลอก **HTTP API Token** ที่ได้มาใช้งาน
- ค้นหา **User ID** หรือ **Group ID** ของคุณได้โดยการคุยกับ [@userinfobot](https://t.me/userinfobot) (หากแชทกลุ่มให้เชิญบอทดังกล่าวหรือบอทหา ID เข้าไปในกลุ่ม) *หมายเหตุ: ID ของห้องแชทกลุ่ม (Group Chat ID) ใน Telegram จะขึ้นต้นด้วยเครื่องหมายติดลบ (เช่น -100xxxxxxxxxx)*
- **การตั้งค่าใน Airflow**:
  - ไปที่หน้าควบคุม Airflow Console -> **Admin** -> **Connections**
  - สร้าง Connection ใหม่โดยตั้งชื่อ Conn Id เป็น `telegram_default`
  - เลือก Connection Type เป็น `Telegram`
  - นำ **User ID / Group ID** ใส่ไว้ที่ช่อง **`Host`**
  - นำ **Telegram Bot Token** ใส่ไว้ที่ช่อง **`Password`**

---

## ทดลองการแจ้งเตือน `messaging/`


โปรเจกต์นี้รองรับการแจ้งเตือนผลการรันหรือส่งข้อความผ่านแอปพลิเคชัน **LINE** และ **Telegram**

### การตั้งค่า secret สำหรับ messaging

สร้างไฟล์ `.env` ในโฟลเดอร์ `messaging/` (อ้างอิงรูปแบบจาก `.env.example`) เพื่อเก็บค่า Credentials:

```env
LINE_CHANNEL_ACCESS_TOKEN="YOUR_LINE_CHANNEL_ACCESS_TOKEN"
LINE_TARGET_ID="YOUR_LINE_USER_OR_GROUP_ID"
TELEGRAM_TOKEN="YOUR_TELEGRAM_BOT_TOKEN"
TELEGRAM_CHAT_ID="YOUR_TELEGRAM_CHAT_ID"
```

### Testing scripts

1. **`messaging/test_line.py`**:
   - ส่งข้อความ (Push Notification) ไปยัง LINE Chat/Group ผ่าน LINE Messaging API (เนื่องจาก LINE Notify สิ้นสุดการให้บริการแล้ว)

2. **`messaging/test_telegram.py`**:
   - ส่งข้อความตรงหรือส่งเข้ากลุ่ม Telegram ด้วย Telegram Bot API ผ่าน HTTP Request (`requests`)

