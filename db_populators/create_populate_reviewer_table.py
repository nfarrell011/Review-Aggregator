import sqlite3
from pathlib import Path
import os
import csv

# set path to db folder
HOME = Path.cwd()
DB_FOLDER_PATH = HOME / "curated"

# set the path to csv
CSV_FILE_PATH = str(HOME / "raw" / "open_table_review_data_Portland_ME_2024-05-28.csv")

# check if the folder exists
os.makedirs(DB_FOLDER_PATH, exist_ok = True)

# set path to db_file
DB_FILE_PATH = DB_FOLDER_PATH / "restaurant_review_database.db"

# connect to db
connection = sqlite3.connect(str(DB_FILE_PATH))

# define the review table
table = """
            CREATE TABLE IF NOT EXISTS reviewer(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name Text,
                    hometown Text
            )
        """
connection.execute(table)

# write data to the table
inserter = """
                INSERT INTO reviewer(name, 
                                     hometown) VALUES(?,?)
           """

# read in csv, line by line
with open(CSV_FILE_PATH, 'r') as file:
    reader = csv.reader(file)
    next(reader)

    # iterate over the rows extracting data
    for row in reader:

        # row container
        db_row = []

        # extract data
        name = row[8]
        hometown = row[6]

        # add data to container
        db_row.extend([name, hometown])

        # add data to db
        connection.execute(inserter, db_row)
        connection.commit()

# close the connection to db
connection.close()



