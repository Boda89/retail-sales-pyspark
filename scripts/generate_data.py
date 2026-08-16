"""
Generates synthetic retail data for the PySpark project:
- stores.csv       (small lookup table  - ~50 rows)
- products.csv     (small lookup table  - ~200 rows)
- transactions.csv (large fact table    - ~1,500,000 rows)
"""

import numpy as np
import pandas as pd
from faker import Faker
import os

fake = Faker()
np.random.seed(42)

OUTPUT_DIR = "data/raw"
os.makedirs(OUTPUT_DIR, exist_ok=True)

N_STORES = 50
N_PRODUCTS = 200
N_TRANSACTIONS = 1_500_000

CATEGORIES = ["Electronics", "Groceries", "Clothing", "Home & Garden", "Toys", "Sports"]
REGIONS = ["Cairo", "Alexandria", "Giza", "Mansoura", "Aswan"]

# ---------- stores.csv ----------
stores = pd.DataFrame({
    "store_id": range(1, N_STORES + 1),
    "store_name": [f"Store_{i}" for i in range(1, N_STORES + 1)],
    "city": np.random.choice(REGIONS, N_STORES),
})
stores.to_csv(f"{OUTPUT_DIR}/stores.csv", index=False)
print(f"stores.csv written: {len(stores)} rows")

# ---------- products.csv ----------
products = pd.DataFrame({
    "product_id": range(1, N_PRODUCTS + 1),
    "product_name": [fake.unique.word().capitalize() + f"_{i}" for i in range(1, N_PRODUCTS + 1)],
    "category": np.random.choice(CATEGORIES, N_PRODUCTS),
    "unit_price": np.round(np.random.uniform(5, 500, N_PRODUCTS), 2),
})
products.to_csv(f"{OUTPUT_DIR}/products.csv", index=False)
print(f"products.csv written: {len(products)} rows")

# ---------- transactions.csv (the big fact table) ----------
dates = pd.date_range("2025-01-01", "2026-08-01", freq="D")

transactions = pd.DataFrame({
    "transaction_id": range(1, N_TRANSACTIONS + 1),
    "product_id": np.random.randint(1, N_PRODUCTS + 1, N_TRANSACTIONS),
    "store_id": np.random.randint(1, N_STORES + 1, N_TRANSACTIONS),
    "quantity": np.random.randint(1, 10, N_TRANSACTIONS),
    "sale_date": np.random.choice(dates, N_TRANSACTIONS),
})

# ~2% of rows get a null quantity to simulate real-world messiness
null_idx = np.random.choice(N_TRANSACTIONS, size=int(N_TRANSACTIONS * 0.02), replace=False)
transactions.loc[null_idx, "quantity"] = None

transactions.to_csv(f"{OUTPUT_DIR}/transactions.csv", index=False)
print(f"transactions.csv written: {len(transactions)} rows")

print("\nDone. Files are in data/raw/")