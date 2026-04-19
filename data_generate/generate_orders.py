import json
import random
import time
import os
from datetime import datetime
from faker import Faker
import boto3

s3 = boto3.client('s3')

BUCKET_NAME = "ecommerce-data-pipeline-2026"   # 👈 replace this
S3_PREFIX = "raw/orders/"

fake = Faker()

OUTPUT_FOLDER = "orders_data"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

CATEGORIES = ["Electronics", "Clothing", "Home", "Sports"]
PAYMENTS = ["UPI", "Card", "NetBanking", "COD"]
STATUS = ["Placed", "Shipped", "Delivered"]

def generate_clean_order():
    return {
        "order_id": fake.uuid4(),
        "user_id": random.randint(1000, 9999),
        "product_id": random.randint(1, 100),
        "category": random.choice(CATEGORIES),
        "price": round(random.uniform(10, 500), 2),
        "quantity": random.randint(1, 5),
        "order_timestamp": datetime.now().isoformat(),
        "payment_method": random.choice(PAYMENTS),
        "status": random.choice(STATUS)
    }

def introduce_issues(order):
    issue_type = random.choice([
        "missing_field",
        "null_value",
        "wrong_type",
        "schema_variation",
        "clean"
    ])

    if issue_type == "missing_field":
        field_to_remove = random.choice(list(order.keys()))
        order.pop(field_to_remove, None)

    elif issue_type == "null_value":
        field = random.choice(list(order.keys()))
        order[field] = None

    elif issue_type == "wrong_type":
        order["price"] = "unknown"  
        order["quantity"] = "two"   

    elif issue_type == "schema_variation":
    
        order["userID"] = order.pop("user_id")
        order["productID"] = order.pop("product_id")
        order["order_time"] = order.pop("order_timestamp")

    return order

def generate_order():
    order = generate_clean_order()
    
    # 40% chance to introduce bad data
    if random.random() < 0.4:
        order = introduce_issues(order)
    
    return order

def generate_batch(batch_size=100):
    return [generate_order() for _ in range(batch_size)]

def save_to_file(data):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"orders_{timestamp}.json"
    file_path = os.path.join(OUTPUT_FOLDER, file_name)

    # Save locally
    with open(file_path, "w") as f:
        json.dump(data, f, indent=4)

    print(f"Saved locally: {file_path}")

    # Upload to S3
    s3_key = S3_PREFIX + file_name

    try:
        s3.upload_file(file_path, BUCKET_NAME, s3_key)
        print(f"Uploaded to S3: {s3_key}")
    except Exception as e:
        print("S3 Upload Failed:", e)

if __name__ == "__main__":
    while True:
        batch_data = generate_batch(batch_size=100)
        save_to_file(batch_data)
        time.sleep(5)
