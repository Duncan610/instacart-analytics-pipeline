import os
import logging
import argparse
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


FILE_CONFIGS = {
    "orders.csv": {
        "table": "orders",
        "dtype": {
            "order_id": "Int64",
            "user_id": "Int64",
            "order_number": "Int64",
            "order_dow": "Int64",
            "order_hour_of_day": "Int64",
            "days_since_prior_order": "Float64",
        },
    },
    "order_products__prior.csv": {
        "table": "order_products_prior",
        "dtype": {
            "order_id": "Int64",
            "product_id": "Int64",
            "add_to_cart_order": "Int64",
            "reordered": "Int64",
        },
    },
    "order_products__train.csv": {
        "table": "order_products_train",
        "dtype": {
            "order_id": "Int64",
            "product_id": "Int64",
            "add_to_cart_order": "Int64",
            "reordered": "Int64",
        },
    },
    "products.csv": {
        "table": "products",
        "dtype": {
            "product_id": "Int64",
            "aisle_id": "Int64",
            "department_id": "Int64",
        },
    },
    "aisles.csv": {
        "table": "aisles",
        "dtype": {
            "aisle_id": "Int64",
        },
    },
    "departments.csv": {
        "table": "departments",
        "dtype": {
            "department_id": "Int64",
        },
    },
}

CHUNK_SIZE = 100_000


def get_engine():
    """
    SQLAlchemy connection engine from environment variables.
    Reads from .env file via python-dotenv.
    """
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "instacart")
    user = os.getenv("POSTGRES_USER", "analytics")
    password = os.getenv("POSTGRES_PASSWORD", "analytics")

    connection_string = (
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    )

    logger.info(f"Connecting to PostgreSQL at {host}:{port}/{db}")
    return create_engine(connection_string, echo=False)


def ensure_schema(engine, schema: str = "raw"):
    """
    Create the raw schema if it doesn't exist.
    """
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        conn.commit()
    logger.info(f"Schema '{schema}' is ready.")


def load_file(
    engine,
    filepath: Path,
    table: str,
    dtype: dict,
    schema: str = "raw"
):
    """
    Load a single CSV file into PostgreSQL.

    - Reads the file in chunks to avoid memory issues with large files
    - Adds audit columns (_loaded_at, _source_file) to every row
    - First chunk uses if_exists='replace' to create/overwrite the table
    - Subsequent chunks use if_exists='append' to add rows
    """
    logger.info(f"Starting: {filepath.name} → {schema}.{table}")
    loaded_at = datetime.now(timezone.utc)

    total_rows = 0
    first_chunk = True

    for chunk in pd.read_csv(
        filepath,
        dtype=dtype,
        chunksize=CHUNK_SIZE
    ):
        # Add audit columns so we know when and where data came from
        chunk["_loaded_at"] = loaded_at
        chunk["_source_file"] = filepath.name

        # First chunk: replace table if it exists (fresh load)
        if_exists = "replace" if first_chunk else "append"

        chunk.to_sql(
            name=table,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            method="multi",
        )

        total_rows += len(chunk)
        first_chunk = False
        logger.info(f"  → {total_rows:,} rows loaded so far...")

    logger.info(
        f" {filepath.name}: {total_rows:,} total rows "
        f"loaded into {schema}.{table}"
    )
    return total_rows


def main(data_dir: str, schema: str = "raw"):
    """
    Main function: connects to PostgreSQL, ensures schema exists,
    then loads each CSV file in FILE_CONFIGS order.
    """
    data_path = Path(data_dir)

    # Verify the data directory exists
    if not data_path.exists():
        logger.error(f"Data directory not found: {data_path}")
        logger.error("Check your --data-dir path and try again.")
        return

    engine = get_engine()
    ensure_schema(engine, schema)

    results = {}
    failed = []

    for filename, config in FILE_CONFIGS.items():
        filepath = data_path / filename

        if not filepath.exists():
            logger.warning(f"File not found: {filepath} — skipping.")
            failed.append(filename)
            continue

        try:
            rows = load_file(
                engine,
                filepath,
                config["table"],
                config["dtype"],
                schema,
            )
            results[filename] = rows

        except Exception as e:
            logger.error(f"Failed to load {filename}: {e}")
            failed.append(filename)

    logger.info("")
    logger.info("=" * 60)
    logger.info("INGESTION SUMMARY")
    logger.info("=" * 60)

    for filename, rows in results.items():
        logger.info(f"  {filename:<45} {rows:>12,} rows")

    logger.info("-" * 60)
    logger.info(
        f"  {'TOTAL':<45} {sum(results.values()):>12,} rows"
    )

    if failed:
        logger.info("")
        logger.warning(f"  SKIPPED/FAILED: {', '.join(failed)}")

    logger.info("=" * 60)

    if not failed:
        logger.info("")
        logger.info("All files loaded successfully.")
        logger.info("Next step: cd dbt_project && dbt debug")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load Instacart CSVs into PostgreSQL raw schema"
    )
    parser.add_argument(
        "--data-dir",
        default="/home/otieno/Downloads/instadata",
        help="Folder containing the Instacart CSV files"
    )
    parser.add_argument(
        "--schema",
        default="raw",
        help="PostgreSQL schema to load data into (default: raw)"
    )
    args = parser.parse_args()

    main(args.data_dir, args.schema)