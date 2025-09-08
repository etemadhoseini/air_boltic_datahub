import os
import sys
import json
from pathlib import Path
from typing import List, Dict, Any, Iterable

import pandas as pd
from databricks import sql

CATALOG = os.environ.get("RAW_CATALOG", "raw")
DATA_DIR = Path(__file__).parent
BATCH_SIZE = int(os.environ.get("INSERT_BATCH_SIZE", "1000"))

# ---- Helpers -----------------------------------------------------------------

def die(msg: str, code: int = 1):
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)

def env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        die(f"Missing env var: {name}")
    return val

def sanitize_identifier(name: Any) -> str:
    """
    Convert arbitrary column names to SQL-safe identifiers.
    Lowercase, spaces->underscore, remove invalid chars, collapse repeats.
    """
    s = str(name).strip().lower()
    # replace spaces and slashes with underscore
    for ch in [" ", "/", "\\", "-", "."]:
        s = s.replace(ch, "_")
    # keep alnum and underscore only
    s = "".join(c for c in s if c.isalnum() or c == "_")
    # collapse multiple underscores
    while "__" in s:
        s = s.replace("__", "_")
    # must start with a letter or underscore
    if not s or not (s[0].isalpha() or s[0] == "_"):
        s = "_" + s
    return s

def df_all_string(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy where all columns are strings and NaNs preserved as None."""
    # Ensure columns are strings and sanitized
    df = df.copy()
    df.columns = [sanitize_identifier(c) for c in df.columns]
    # Convert values: NaN/NaT -> None, else str
    def to_str_or_none(x):
        if pd.isna(x):
            return None
        return str(x)
    for col in df.columns:
        df[col] = df[col].map(to_str_or_none)
    return df

def infer_create_table_sql(full_name: str, columns: List[str]) -> str:
    cols_sql = ", ".join(f"`{c}` STRING" for c in columns)
    return f"CREATE OR REPLACE TABLE {full_name} ({cols_sql})"

def insert_rows(cur, full_name: str, columns: List[str], rows: Iterable[Iterable[Any]]):
    placeholders = ", ".join(["?"] * len(columns))
    cols = ", ".join(f"`{c}`" for c in columns)
    sql_stmt = f"INSERT INTO {full_name} ({cols}) VALUES ({placeholders})"
    batch = []
    count = 0
    for row in rows:
        batch.append(tuple(row))
        if len(batch) >= BATCH_SIZE:
            cur.executemany(sql_stmt, batch)
            count += len(batch)
            print(f"  inserted {count} rows into {full_name} ...")
            batch = []
    if batch:
        cur.executemany(sql_stmt, batch)
        count += len(batch)
    print(f"  total inserted: {count} rows into {full_name}")

def ensure_schemas(cur):
    for schema in ["demand", "supply", "operations"]:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{schema}`")
    print(f"Ensured schemas in catalog `{CATALOG}`: demand, supply, operations")

def load_excel_frames(xlsx_path: Path) -> Dict[str, pd.DataFrame]:
    print(f"Reading Excel file: {xlsx_path}")
    xl = pd.ExcelFile(xlsx_path)
    wanted = ["customer", "customer_group", "airplane", "trip", "order"]
    frames = {}
    # map sheet names case-insensitively
    sheet_map = {s.lower(): s for s in xl.sheet_names}
    for w in wanted:
        if w in sheet_map:
            frames[w] = xl.parse(sheet_map[w])
        else:
            print(f"  WARNING: sheet '{w}' not found in workbook; skipping.")
    return frames

def load_airplane_model_json(json_path: Path) -> pd.DataFrame:
    print(f"Reading airplane model JSON: {json_path}")
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    rows = []
    for manufacturer, models in data.items():
        if not isinstance(models, dict):
            continue
        for model, attrs in models.items():
            row = {
                "manufacturer": manufacturer,
                "model": model,
            }
            if isinstance(attrs, dict):
                for k, v in attrs.items():
                    row[sanitize_identifier(k)] = None if v is None else str(v)
            rows.append(row)

    if not rows:
        return pd.DataFrame(columns=["manufacturer", "model"])

    # Align columns
    df = pd.DataFrame(rows)
    df = df_all_string(df)
    # Ensure predictable column order
    base_cols = ["manufacturer", "model"]
    other_cols = sorted([c for c in df.columns if c not in base_cols])
    return df[base_cols + other_cols]

def find_first(path_globs: List[str]) -> Path | None:
    for pat in path_globs:
        matches = list(DATA_DIR.glob(pat))
        if matches:
            return matches[0]
    return None

def ensure_catalogs(cur):
    """
    Ensure the following Unity Catalog catalogs exist:
      - CATALOG (env RAW_CATALOG, default 'raw')
      - dev
      - stg
      - air_boltic

    Optional managed locations (env vars):
      - RAW_CATALOG_LOCATION
      - DEV_CATALOG_LOCATION
      - STG_CATALOG_LOCATION
      - AIR_BOLTIC_CATALOG_LOCATION
    """
    catalogs = [
        (CATALOG, os.environ.get("RAW_CATALOG_LOCATION")),
        ("dev", os.environ.get("DEV_CATALOG_LOCATION")),
        ("stg", os.environ.get("STG_CATALOG_LOCATION")),
        ("air_boltic", os.environ.get("AIR_BOLTIC_CATALOG_LOCATION")),
    ]

    for name, managed_loc in catalogs:
        if managed_loc:
            stmt = f"CREATE CATALOG IF NOT EXISTS `{name}` MANAGED LOCATION '{managed_loc}'"
        else:
            stmt = f"CREATE CATALOG IF NOT EXISTS `{name}`"
        cur.execute(stmt)
        print(
            f"Ensured catalog `{name}`"
            + (f" with managed location {managed_loc}" if managed_loc else "")
        )


# ---- Main --------------------------------------------------------------------

def main():
    host = env("DATABRICKS_HOST")
    http_path = env("DATABRICKS_HTTP_PATH").replace("SQL:", "")
    token = env("DATABRICKS_TOKEN")

    # Locate input files
    excel_path = find_first(["*.xlsx", "*.xlsm", "*.xls"])
    if not excel_path:
        die(f"No Excel workbook found in {DATA_DIR}. Expected sheets: customer, customer_group, airplane, trip, order")
    json_path = find_first(["airplane_model*.json", "aeroplane_model*.json", "*air*plane*model*.json"])
    if not json_path:
        print("WARNING: airplane model JSON not found. Expected something like 'airplane_model.json' or 'aeroplane_model.json'.")

    print(f"Using catalog: `{CATALOG}`")
    print(f"Data directory: {DATA_DIR}")
    print(f"Workbook: {excel_path.name}")
    if json_path:
        print(f"Model JSON: {json_path.name}")

    # Connect
    print("Connecting to Databricks SQL Warehouse ...")
    with sql.connect(server_hostname=host, http_path=http_path, access_token=token) as conn:
        with conn.cursor() as cur:
            ensure_catalogs(cur)
            ensure_schemas(cur)

            # Load Excel sheets
            frames = load_excel_frames(excel_path)

            routing = {
                "customer": ("demand", "customer"),
                "customer_group": ("demand", "customer_group"),
                "airplane": ("supply", "airplane"),
                "trip": ("operations", "trip"),
                "order": ("operations", "order"),
            }

            # Upload each sheet
            for sheet, df in frames.items():
                schema, table = routing[sheet]
                full_name = f"`{CATALOG}`.`{schema}`.`{table}`"
                print(f"\nProcessing sheet '{sheet}' -> {full_name}")

                df = df_all_string(df)

                # Build / replace table with STRING columns
                create_sql = infer_create_table_sql(full_name, list(df.columns))
                cur.execute(create_sql)
                conn.commit()

                # Insert rows in batches
                insert_rows(cur, full_name, list(df.columns), df.itertuples(index=False, name=None))
                conn.commit()

            # JSON -> airplane_model table in supply schema
            if json_path:
                df_json = load_airplane_model_json(json_path)
                schema, table = "supply", "airplane_model"
                full_name = f"`{CATALOG}`.`{schema}`.`{table}`"
                print(f"\nProcessing JSON 'airplane_model' -> {full_name}")

                create_sql = infer_create_table_sql(full_name, list(df_json.columns))
                cur.execute(create_sql)
                conn.commit()

                insert_rows(cur, full_name, list(df_json.columns), df_json.itertuples(index=False, name=None))
                conn.commit()

    print("\nAll done ✅")

if __name__ == "__main__":
    # Ensure data dir exists
    if not DATA_DIR.exists():
        die(f"Data directory not found: {DATA_DIR}")
    main()
