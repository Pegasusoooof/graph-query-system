import os
import json
import pandas as pd
from sqlalchemy import create_engine

RAW_DATA_PATH = "../raw-data"

def load_jsonl_folder(folder_path):
    records = []
    for file in os.listdir(folder_path):
        if file.endswith(".jsonl"):
            with open(os.path.join(folder_path, file)) as f:
                for line in f:
                    records.append(json.loads(line))
    return pd.DataFrame(records)

# TEST: load one dataset
DB_URL = "postgresql://postgres:Shreyas%40123098@db.mitekbfjinnblwzwswsd.supabase.co:5432/postgres"
engine = create_engine(DB_URL)
for folder_name in os.listdir(RAW_DATA_PATH):
    folder_path = os.path.join(RAW_DATA_PATH, folder_name)

    if os.path.isdir(folder_path):
        df = load_jsonl_folder(folder_path)

        print(f"Loading {folder_name}... rows={len(df)}")

        df = df.apply(lambda col: col.map(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x))

        df.to_sql(folder_name, engine,
                  if_exists="replace",
                  index=False)

print(df.head())
print(df.columns)