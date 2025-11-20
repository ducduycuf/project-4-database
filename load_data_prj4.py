import os
import psycopg2
import pandas as pd

DATA_FOLDER = "./"

conn = psycopg2.connect(
    dbname="ecommerce",
    user="postgres",
    password="123456",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

insert_order = [
    "orders.csv",
    "order_items.csv"
]

for file in insert_order:
    csv_path = os.path.join(DATA_FOLDER, file)
    table_name = file.replace(".csv", "")

    print(f"→ Loading {file} into table {table_name}...")

    df = pd.read_csv(csv_path)

    columns = ",".join(df.columns)
    placeholders = ",".join(["%s"] * len(df.columns))

    for row in df.itertuples(index=False, name=None):
        cur.execute(
            f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})",
            row
        )

    conn.commit()
    print(f"Successfully inserted into {table_name}\n")

cur.close()
conn.close()