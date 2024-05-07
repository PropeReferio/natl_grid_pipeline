import requests
import sqlite3
from typing import List, Optional
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from tenacity import retry, wait_exponential, stop_after_attempt

from db import create_natl_grid_auction_results_table_if_not_exists, DB_NAME

utc_today = datetime.utcnow()
RESOURCE_ID = "a63ab354-7e68-44c2-ad96-c6f920c30e85"
AUCTION_PARTICIPANT = "HABITAT ENERGY LIMITED"
BASE_URL = "https://api.nationalgrideso.com/api/3/action/datastore_search_sql"
URL_PARAM_SQL = [
    f"?sql=select * from \"{RESOURCE_ID}\" ",
    f"WHERE \"registeredAuctionParticipant\" = '{AUCTION_PARTICIPANT}' ",
    f"AND \"deliveryStart\" >= '{utc_today.date()}' ",
    f"AND \"deliveryStart\" < '{(utc_today + timedelta(days=1)).date()}'"
]
# https://api.nationalgrideso.com/api/3/action/datastore_search_sql?sql=select%20*%20from%20%22a63ab354-7e68-44c2-ad96-c6f920c30e85%22%20WHERE%20%22registeredAuctionParticipant%22%20=%20%27HABITAT%20ENERGY%20LIMITED%27%20LIMIT%20500
# 14k records with no limit.


@dataclass
class AuctionRecord:
    natl_grid_id: int
    auction_unit: str
    service_type: str
    auction_product: str
    executed_quantity: int
    clearing_price: float
    delivery_start: str
    delivery_end: str
    technology_type: str
    post_code: str
    unit_result_id: str
    full_text: str
    date_ingested: str

    def __repr__(self):
        return (
            f"Date Ingested: {self.date_ingested} "
            f"Executed Quantity: {self.executed_quantity} "
            f"Clearing Price: {self.clearing_price} "
            f"Delivery Start: {self.delivery_start} "
            f"Post Code: {self.post_code} "
        )


class Api:

    def __init__(self):
        create_natl_grid_auction_results_table_if_not_exists()

    @retry(
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(5)
    )
    def list_daily_auction_results_from_page(self) -> List[AuctionRecord]:
        """
        List a page of records from National Grid. Paginate based on
        records already ingested.
        :return: A list of AuctionRecords, to be written to the DB.
        """
        records = []
        response = requests.get(BASE_URL + ''.join(URL_PARAM_SQL))
        resp_json = response.json()
        for record in resp_json["result"]["records"]:
            records.append(
                AuctionRecord(
                    record["_id"],
                    record["auctionUnit"],
                    record["serviceType"],
                    record["auctionProduct"],
                    int(float(record["executedQuantity"])),
                    float(record["clearingPrice"]),
                    record["deliveryStart"],
                    record["deliveryEnd"],
                    record["technologyType"],
                    record["postCode"],
                    record["unitResultID"],
                    record["_full_text"],
                    str(utc_today.date())
                )
            )

        return records

    # @retry(
    #     wait=wait_exponential(multiplier=1, min=4, max=10),
    #     stop=stop_after_attempt(5)
    # )
    def write_records_to_database(self, records: List[AuctionRecord]):
        """
        Write a page of records to the Database.
        """
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()
            # for record in records:
            cursor.executemany(
                """
                INSERT INTO natl_grid_auction_results 
                (natl_grid_id, auction_unit, service_type, auction_product, executed_quantity, clearing_price, 
                delivery_start, delivery_end, technology_type, post_code, unit_result_id, full_text, date_ingested) 
                VALUES (:natl_grid_id, :auction_unit, :service_type, :auction_product, :executed_quantity, 
                :clearing_price, :delivery_start, :delivery_end, :technology_type, :post_code, :unit_result_id, 
                :full_text, :date_ingested)
                """,
                [asdict(record) for record in records]
            )

            conn.commit()
        # customer_data = [('John Doe', 'john.doe@email.com'), ('Jane Smith', 'jane.smith@email.com')]
        # cursor.executemany("INSERT INTO customers (name, email) VALUES (?, ?)", customer_data)

    def read_and_write_records(self):
        records = self.list_daily_auction_results_from_page()
        self.write_records_to_database(records)


def main():
    create_natl_grid_auction_results_table_if_not_exists()
    api = Api()
    api.read_and_write_records()


if __name__ == "__main__":
    main()