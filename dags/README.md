# ตัวอย่าง Airflow AI DAGs

โฟลเดอร์นี้ประกอบด้วยตัวอย่าง Apache Airflow DAGs ที่แสดงวิธีการเชื่อมต่อกับโมเดล AI ของ Google (Gemini) ผ่านการใช้งาน `@task.llm` decorator

## ไฟล์ที่มีอยู่ในโฟลเดอร์

- **`dag1_simple_ai.py` (DAG: `ai_test`)**: ตัวอย่างการทำงานพื้นฐานที่แสดงการส่งคำถามสั้นๆ ไปยัง AI (เช่น ถามว่า "Apache Airflow คืออะไร สรุป 1 ประโยค") และพิมพ์คำตอบที่ได้ออกมา

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

- **`dag2_ai_spam_filter.py` (DAG: `ai_spam_filter`)**: เวิร์กโฟลว์ขั้นสูงที่ทำงานเป็นตัวตรวจสอบคุณภาพข้อมูล (Data Quality checker) โดยโปรแกรมจะอ่านข้อมูลรีวิวจากไฟล์ CSV จากนั้นส่งให้ AI ช่วยประเมินความเป็นสแปมของรีวิวแต่ละอัน และรับคำตอบกลับมาในรูปแบบ JSON (ประกอบด้วยคะแนนสแปมและเหตุผลภาษาไทย)

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

- **`reviews.csv`**: ไฟล์ข้อมูลรีวิวตัวอย่างที่ใช้ใน DAG `ai_spam_filter`
