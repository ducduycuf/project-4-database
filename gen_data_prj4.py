from faker import Faker
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

faker = Faker()

# Generate orders
def generate_orders(n, sellers_df):
    status_choices = ["PLACED", "PAID", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]
    weights = [0.10, 0.65, 0.05, 0.05, 0.10, 0.05]

    # Convert date range
    start_date = datetime(2025, 8, 1)
    end_date = datetime(2025, 10, 31)
    total_days = (end_date - start_date).days

    seller_ids = sellers_df["seller_id"].tolist()

    order_dates = [
        start_date + timedelta(days=random.randint(0, total_days),
                               seconds=random.randint(0, 86399))
        for _ in range(n)
    ]

    return pd.DataFrame({
        "order_id": np.arange(1, n + 1),
        "order_date": order_dates,
        "seller_id": np.random.choice(seller_ids, size=n),
        "status": random.choices(status_choices, weights=weights, k=n),
        "total_amount": np.round(np.random.uniform(50_000, 50_000_000, size=n), 2),
        "created_at": [faker.date_time_between(start_date="-1y", end_date="now") for _ in range(n)]
    })

# Generate order_items (product.seller_id == order.seller_id)
def generate_order_items(orders_df, products_df):
    records = []

    # Map seller_id → list of product_ids
    seller_to_products = {
        sid: df['product_id'].tolist()
        for sid, df in products_df.groupby("seller_id")
    }

    prod_df = products_df.set_index("product_id")

    for _, order in orders_df.iterrows():
        seller_id = order["seller_id"]

        valid_products = seller_to_products.get(seller_id, [])

        available = len(valid_products)

        # seller has 0 products → skip order
        if available == 0:
            continue

        # seller has only 1 product → forced 1 item
        if available == 1:
            chosen_ids = valid_products

        # seller has 2+ products → pick 2–4 items safely
        else:
            num_items = random.randint(2, min(4, available))
            chosen_ids = random.sample(valid_products, num_items)

        for pid in chosen_ids:
            quantity = random.randint(1, 5)
            unit_price = float(prod_df.loc[pid, "discount_price"])
            subtotal = round(quantity * unit_price, 2)

            records.append({
                "order_item_id": None,
                "order_id": order["order_id"],
                "product_id": pid,
                "quantity": quantity,
                "unit_price": unit_price,
                "subtotal": subtotal,
                "created_at": order["order_date"]
            })

    oi = pd.DataFrame(records)
    oi = oi.reset_index(drop=True)
    oi["order_item_id"] = oi.index + 1
    return oi


if __name__ == "__main__":
    sellers_df = pd.read_csv("sellers_table.csv")
    products_df = pd.read_csv("products_table.csv")

    print("Generating orders...")
    orders_df = generate_orders(3_000_000, sellers_df)
    print("Orders generated:", len(orders_df))

    print("Generating order items...")
    order_items_df = generate_order_items(orders_df, products_df)
    print("Order items generated:", len(order_items_df))

    print("Saving output files...")
    orders_df.to_csv("orders.csv", index=False)
    order_items_df.to_csv("order_items.csv", index=False)

    print("DONE.")