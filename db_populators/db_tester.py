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

# connect to db
connection = sqlite3.connect(str(DB_FILE_PATH))

# define query
query = """
            SELECT * FROM reviewer
        """

# execute query
result = connection.execute(query)
data = result.fetchall()

# display data
for index, row in enumerate(data):
    if index < 20:
        print(row)
        print()
    else:
        break

connection.close()