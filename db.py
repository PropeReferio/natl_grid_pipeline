import sqlite3

DB_NAME = 'habitat.db'


def create_natl_grid_auction_results_table_if_not_exists():
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS natl_grid_auction_results (
            _id INTEGER PRIMARY KEY,
            natl_grid_id INTEGER,
            auction_unit TEXT,
            service_type TEXT,
            auction_product TEXT,
            executed_quantity INTEGER,
            clearing_price REAL,
            delivery_start TEXT,
            delivery_end TEXT,
            technology_type TEXT,
            post_code TEXT,
            unit_result_id TEXT,
            full_text TEXT,
            date_ingested TEXT
            )
            """
        )

        conn.commit()
