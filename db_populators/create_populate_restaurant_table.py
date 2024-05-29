import sqlite3
from pathlib import Path
import os
import csv

# set path to db folder
HOME = Path.cwd()
DB_FOLDER_PATH = HOME / "curated"

# check if folder exists
os.makedirs(DB_FOLDER_PATH, exist_ok = True)

# set path to db_file
DB_FILE_PATH = DB_FOLDER_PATH / "restaurant_review_database.db"

# set path to csv file
PATH_TO_CSV = str(HOME / "raw" / "open_table_restaurant_data_Portland_ME_2024-05-28.csv")

# connect to db
connection = sqlite3.connect(str(DB_FILE_PATH))

# define the restaurant info table
table = """
            CREATE TABLE IF NOT EXISTS restaurant(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name Text,
                    price_point Text,
                    cuisine Text,
                    description Text,
                    tags Text,
                    region Text
                )
        """
connection.execute(table)

# write data to the table
inserter = """
                INSERT INTO restaurant(name,
                                            region,
                                            price_point, 
                                            cuisine, 
                                            description, 
                                            tags) VALUES(?,?,?,?,?,?)
           """

# read in csv, line by line
with open(PATH_TO_CSV, 'r') as file:
    reader = csv.reader(file)
    next(reader)

    # iterate over the rows extracting data
    for row in reader:

        # row container
        db_row = []

        # elements
        name = row[6]
        region = row[5]
        price_point = row[1]
        cuisine = row[2]
        des = row[3]
        tags = row[4]

        # add data to the container
        db_row.extend([name, region, price_point, cuisine, des, tags])

        # add data to the db
        connection.execute(inserter, db_row)
        connection.commit()

# close the connection to the db
connection.close()
    




