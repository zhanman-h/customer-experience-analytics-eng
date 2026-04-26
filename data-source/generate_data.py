import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta
import os

fake = Faker()

# CONFIGURATION
COUNTS = {"orders": 1000, "support": 300, "csat": 100}
ERROR_RATE = 0.05 
SUPPORT_REASONS = ['Order not received', 'Wrong Order', 'Incorrect Charge', 'Bought by mistake', 'Damaged Item', 'Delayed Delivery']
CHANNELS = ['phone', 'email', 'live chat']

def generate_data():
    # --- STAGE 1: GENERATE CLEAN DATA FIRST (Ensures IDs exist for linking) ---
    
    # 1. Orders
    orders = []
    for i in range(COUNTS["orders"]):
        orders.append({
            "order_id": f"ORD{1000 + i}",
            "customer_id": f"CUST{random.randint(1, 500)}",
            "customer_name": fake.name(),
            "customer_email": fake.email(),
            "product_id": f"PROD{random.randint(100, 200)}",
            "product_category": random.choice(["Electronics", "Apparel", "Home", "Beauty"]),
            "unit_price": round(random.uniform(20, 500), 2),
            "quantity": random.randint(1, 10),
            "country": fake.country_code(),
            "order_status": random.choice(["Completed", "Cancelled", "Returned"]),
            "order_date": fake.date_between(start_date='-90d', end_date='today')
        })
    df_orders = pd.DataFrame(orders)

    # 2. Support (Links to Orders)
    support = []
    for i in range(COUNTS["support"]):
        created_at = fake.date_time_between(start_date='-90d', end_date='now')
        support.append({
            "chat_id": f"CHT{5000 + i}",
            "order_id": random.choice(df_orders["order_id"].tolist()),
            "channel": random.choice(CHANNELS),
            "contact_reason": random.choice(SUPPORT_REASONS),
            "wait_time": random.randint(10, 600),
            "chat_created_at": created_at,
            "chat_updated_at": created_at + timedelta(minutes=random.randint(5, 120))
        })
    df_support = pd.DataFrame(support)

    # 3. CSAT (Links to Support)
    csat = []
    for i in range(COUNTS["csat"]):
        csat.append({
            "chat_id": random.choice(df_support["chat_id"].tolist()),
            "csat_score": random.randint(1, 5),
            "is_resolved": random.choice(['y', 'n'])
        })
    df_csat = pd.DataFrame(csat)

    # --- STAGE 2: INJECT ERRORS (Post-Generation back-editing) ---
    inject_errors(df_orders, "orders")
    inject_errors(df_support, "support")
    inject_errors(df_csat, "csat")

    # --- STAGE 3: SAVE TO TARGET DIRECTORY ---
    base_path = os.path.dirname(os.path.abspath(__file__))
    target_dir = os.path.join(base_path, "data")
    if not os.path.exists(target_dir): os.makedirs(target_dir)

    df_orders.to_csv(os.path.join(target_dir, "raw_orders.csv"), index=False)
    df_support.to_csv(os.path.join(target_dir, "raw_support.csv"), index=False)
    df_csat.to_csv(os.path.join(target_dir, "raw_csat.csv"), index=False)
    
    print(f"Success! 3 tables generated with {int(ERROR_RATE*100)}% errors.")
    print(f"Files saved to: {target_dir}")