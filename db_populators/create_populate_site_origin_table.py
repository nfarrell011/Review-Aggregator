import sqlite3
from pathlib import Path
import os
import csv

# set path to the db folder
HOME = Path.cwd()
DB_FOLDER_PATH = HOME / "curated"

# set the path to csv
CSV_FILE_PATH = str(HOME / "raw" / "open_table_review_data_Portland_ME_2024-05-28.csv")

# check if the folder exists
os.makedirs(DB_FOLDER_PATH, exist_ok = True)

# set path to db file
DB_FILE_PATH = DB_FOLDER_PATH / "restaurant_review_database.db"

# connect to db
connection = sqlite3.connect(str(DB_FILE_PATH))

# define the table
table = """
            CREATE TABLE IF NOT EXISTS site_origin(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    site_name
            )
        """
connection.execute(table)

# write data to table
inserter = """
                INSERT INTO site_origin(site_name) VALUES(?)
           """


connection.execute(inserter, ["open_table"])
connection.commit()
connection.close()



