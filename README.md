## National Grid ESO Pipeline
This ingests the daily results of auctions/bids from National Grid.

#### Installation
1. Install Python. https://www.python.org/downloads/
2. In a terminal, run the following in the project directory:
```commandline
python -m pip install -r requirements.txt
python ./ingest.py
```

#### Data
This uses SQLite as the DB. https://www.sqlite.org/index.html

You can query SQLite a number of ways, here's an example:
```commandline
sqlite3 habitat.db <<EOF
SELECT * FROM natl_grid_auction_results;
EOF
```