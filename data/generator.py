#!/usr/bin/env bash
""":"
exec .venv/bin/python "$0" "$@"
"""
"""
data/generator.py
Synthetic E-Commerce Data Generator for Apache Spark Masterclass.
Generates:
  - data/raw/customers.csv
  - data/raw/products.parquet
  - data/raw/orders.json
  - data/raw/order_items.csv
  - data/raw/clickstream.json
  - data/raw/product_reviews.txt

Usage:
  python data/generator.py --scale 1       # Default base size (~1k customers, 5k orders)
  python data/generator.py --scale 10      # 10x size for performance & shuffle tuning
"""

import argparse
import json
import os
import random
from datetime import datetime, timedelta
from faker import Faker
import pandas as pd

def generate_datasets(scale: float = 1.0, output_dir: str = "data/raw"):
    os.makedirs(output_dir, exist_ok=True)
    fake = Faker()
    Faker.seed(42)
    random.seed(42)

    num_customers = int(1000 * scale)
    num_products = int(200 * scale)
    num_orders = int(5000 * scale)
    num_clickstream = int(25000 * scale)
    num_reviews = int(3000 * scale)

    print(f"[*] Generating dataset with scale={scale}x into '{output_dir}'...")
    print(f"    - Customers: {num_customers}")
    print(f"    - Products: {num_products}")
    print(f"    - Orders: {num_orders}")
    print(f"    - Clickstream events: {num_clickstream}")
    print(f"    - Reviews: {num_reviews}")

    # 1. Customers
    loyalty_tiers = ["Bronze", "Silver", "Gold", "Platinum"]
    tier_weights = [0.50, 0.30, 0.15, 0.05]
    start_date = datetime(2023, 1, 1)

    customers = []
    for c_id in range(1, num_customers + 1):
        signup_dt = start_date + timedelta(days=random.randint(0, 500))
        customers.append({
            "customer_id": c_id,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "signup_date": signup_dt.strftime("%Y-%m-%d"),
            "loyalty_tier": random.choices(loyalty_tiers, weights=tier_weights)[0]
        })
    df_customers = pd.DataFrame(customers)
    customers_path = os.path.join(output_dir, "customers.csv")
    df_customers.to_csv(customers_path, index=False)
    print(f"[✓] Saved {customers_path}")

    # 2. Products
    categories = ["Electronics", "Books", "Home & Kitchen", "Apparel", "Sports & Outdoors", "Beauty"]
    products = []
    for p_id in range(1, num_products + 1):
        cat = random.choice(categories)
        price = round(random.uniform(5.0, 500.0), 2)
        cost = round(price * random.uniform(0.4, 0.75), 2)
        products.append({
            "product_id": p_id,
            "name": f"{fake.word().capitalize()} {fake.word().capitalize()}",
            "category": cat,
            "price": price,
            "cost": cost,
            "stock_quantity": random.randint(10, 1000),
            "supplier_id": random.randint(1, 20)
        })
    df_products = pd.DataFrame(products)
    products_path = os.path.join(output_dir, "products.parquet")
    df_products.to_parquet(products_path, index=False)
    print(f"[✓] Saved {products_path}")

    # 3. Orders & Order Items
    order_statuses = ["COMPLETED", "SHIPPED", "PENDING", "CANCELLED", "REFUNDED"]
    status_weights = [0.65, 0.15, 0.10, 0.07, 0.03]
    payment_methods = ["CREDIT_CARD", "PAYPAL", "DEBIT_CARD", "APPLE_PAY", "CRYPTO"]

    orders = []
    order_items = []
    order_item_id = 1

    for o_id in range(1, num_orders + 1):
        c_id = random.randint(1, num_customers)
        o_date = start_date + timedelta(days=random.randint(0, 600), hours=random.randint(0, 23), minutes=random.randint(0, 59))
        status = random.choices(order_statuses, weights=status_weights)[0]
        payment = random.choice(payment_methods)
        shipping_fee = 0.0 if random.random() > 0.4 else 9.99

        num_items = random.randint(1, 5)
        order_total = 0.0
        for _ in range(num_items):
            prod = random.choice(products)
            qty = random.randint(1, 4)
            unit_price = prod["price"]
            discount = round(unit_price * qty * (0.1 if random.random() < 0.2 else 0.0), 2)
            item_total = (unit_price * qty) - discount
            order_total += item_total

            order_items.append({
                "order_item_id": order_item_id,
                "order_id": o_id,
                "product_id": prod["product_id"],
                "quantity": qty,
                "unit_price": unit_price,
                "discount": discount
            })
            order_item_id += 1

        orders.append({
            "order_id": o_id,
            "customer_id": c_id,
            "order_date": o_date.strftime("%Y-%m-%d %H:%M:%S"),
            "status": status,
            "payment_method": payment,
            "shipping_fee": shipping_fee,
            "total_amount": round(order_total + shipping_fee, 2)
        })

    orders_path = os.path.join(output_dir, "orders.json")
    with open(orders_path, "w") as f:
        for o in orders:
            f.write(json.dumps(o) + "\n")
    print(f"[✓] Saved {orders_path}")

    order_items_path = os.path.join(output_dir, "order_items.csv")
    pd.DataFrame(order_items).to_csv(order_items_path, index=False)
    print(f"[✓] Saved {order_items_path}")

    # 4. Clickstream Events (JSON Lines with nested objects)
    event_types = ["page_view", "add_to_cart", "remove_from_cart", "search", "checkout_step"]
    device_types = ["mobile", "desktop", "tablet"]
    clickstream_path = os.path.join(output_dir, "clickstream.json")

    with open(clickstream_path, "w") as f:
        for ev_id in range(1, num_clickstream + 1):
            c_id = random.randint(1, num_customers) if random.random() > 0.15 else None
            ev_type = random.choice(event_types)
            ev_time = start_date + timedelta(days=random.randint(0, 600), seconds=random.randint(0, 86400))
            record = {
                "event_id": f"evt_{ev_id:08d}",
                "customer_id": c_id,
                "session_id": f"sess_{random.randint(1, max(10, num_customers // 2)):06d}",
                "timestamp": ev_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "event_type": ev_type,
                "page_url": f"/shop/{random.choice(categories).lower().replace(' ', '-')}",
                "device_type": random.choice(device_types),
                "ip_address": fake.ipv4(),
                "metadata": {
                    "product_id": random.randint(1, num_products) if ev_type in ["add_to_cart", "page_view"] else None,
                    "search_query": fake.word() if ev_type == "search" else None,
                    "referrer": fake.uri() if random.random() > 0.5 else "direct"
                }
            }
            f.write(json.dumps(record) + "\n")
    print(f"[✓] Saved {clickstream_path}")

    # 5. Product Reviews (Text with structured and unstructured fields)
    sample_positive = [
        "Absolutely love this item! Highly recommend to everyone.",
        "Great build quality, fast shipping, works as described.",
        "Exceeded my expectations! Will buy again for gifts.",
        "Five stars, flawless design and easy to use."
    ]
    sample_negative = [
        "Defective on arrival, poor customer support.",
        "Not worth the price, cheap materials and broke after two days.",
        "Completely disappointed. Does not match the description.",
        "Terrible experience, would not recommend to anyone."
    ]
    sample_neutral = [
        "It's okay for the price, nothing special.",
        "Average product, does the job but could be improved.",
        "Shipped late, but product quality is acceptable."
    ]

    reviews_path = os.path.join(output_dir, "product_reviews.txt")
    with open(reviews_path, "w") as f:
        for r_id in range(1, num_reviews + 1):
            rating = random.randint(1, 5)
            if rating >= 4:
                text = random.choice(sample_positive)
            elif rating <= 2:
                text = random.choice(sample_negative)
            else:
                text = random.choice(sample_neutral)
            
            p_id = random.randint(1, num_products)
            c_id = random.randint(1, num_customers)
            r_date = (start_date + timedelta(days=random.randint(10, 600))).strftime("%Y-%m-%d")
            f.write(f"{r_id}\t{p_id}\t{c_id}\t{rating}\t{r_date}\t{text}\n")
    print(f"[✓] Saved {reviews_path}")
    print("[*] All datasets generated successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce data for Spark demo.")
    parser.add_argument("--scale", type=float, default=1.0, help="Scale multiplier for data volume (default: 1.0)")
    parser.add_argument("--output", type=str, default="data/raw", help="Target directory for raw data")
    args = parser.parse_args()
    generate_datasets(scale=args.scale, output_dir=args.output)
