# Customer Experience & Revenue-at-Risk Analytics

## 📌 Project Overview
This project demonstrates an end-to-end analytics engineering workflow. It utilizes a **Python Faker** script to generate synthetic transactional and support data, which is then modeled in **PostgreSQL** using **dbt**. 

The pipeline transforms raw data into a high-performance star schema to analyze how **support latency (wait times)** and **resolution efficiency** impact **Customer Satisfaction (CSAT)** and **Revenue**.

## 🛠 Tech Stack
* **Data Generation:** Python (Faker Library)
* **Database:** PostgreSQL (OLAP)
* **Transformation:** dbt (Data Build Tool)
* **Containerization:** Docker & Docker Compose
* **Version Control:** Git & GitHub

## 📊 Data Architecture & Modeling
The project follows the modular dbt modeling structure:

1.  **Staging Layer (`stg_`):** Initial cleanup of raw source tables (orders, support, csat). Renaming columns, casting data types, and basic PII masking.
2.  **Intermediate Layer (`int_`):** Complex joins and business logic, such as calculating "Time to Resolve" and "SLA Breaches."
3.  **Mart Layer (`fct_` & `dim_`):** Final, denormalized tables optimized for BI tools.
    * `fct_customer_experience`: The central fact table linking revenue to support sentiment.
    * `dim_customers`: Hashed PII with geographic and demographic attributes.
    * `dim_products`: Product categories, pricing, and unit analysis.

## 🐍 Data Generation (Python Faker)
To simulate a real-world scenario, a Python script (`generate_data.py`) generates:
* **Orders:** Randomized across countries and categories with variable pricing and units.
* **Support Tickets:** Logic-based generation where certain order delays trigger support chats.
* **CSAT:** Sentiment scores correlated to support wait times and resolution status.

## 🚀 Key Metrics Built
1.  **SLA Breach Rate:** % of chats where `wait_time` exceeded the service threshold.
2.  **Revenue-at-Risk:** Total GMV associated with unresolved tickets or low CSAT scores.
3.  **Resolution Efficiency:** Time elapsed from `chat_created_at` to `chat_updated_at`.
