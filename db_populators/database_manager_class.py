import sqlite3
from pathlib import Path
import os
import csv


class RestaurantReviewDB:
    """
        
    """

    def __init__(self, db_file_name) -> None:
        """
        
        """
        self.HOME = Path.cwd()
        self.file_name = db_file_name
        self.connection = None
        self.cur = None
    
    def connect(self):
        """
        
        """
        # set the path to the db folder
        DB_FOLDER_PATH = self.HOME / "data" / "curated"

        # check if the folder exists, if not create
        os.makedirs(DB_FOLDER_PATH, exist_ok = True)

        # set path to db file
        DB_FILE_PATH = DB_FOLDER_PATH / self.file_name

        # connect to db
        self.connection = sqlite3.connect(str(DB_FILE_PATH))
        self.cur = self.connection.cursor()

    def create_tables(self):
        """
        
        """
        # connect to db
        self.connect()

        # define the tables
        create_site_origin_table =      """
                                        CREATE TABLE IF NOT EXISTS site_origin(
                                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                site_name TEXT
                                                )
                                        """
        create_reviewer_table =         """
                                        CREATE TABLE IF NOT EXISTS reviewer(
                                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                name TEXT,
                                                hometown TEXT
                                                )
                                        """
        create_restaurant_table =       """
                                        CREATE TABLE IF NOT EXISTS restaurant(
                                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                name TEXT,
                                                price_point TEXT,
                                                cuisine TEXT,
                                                description TEXT,
                                                tags TEXT,
                                                region TEXT
                                                )
                                        """
        create_res_review_table =       """
                                        CREATE TABLE IF NOT EXISTS res_review(
                                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                restaurant_id INTEGER,
                                                reviewer_id INTEGER,
                                                site_origin_id INTEGER,
                                                rating INTEGER,
                                                date TEXT,
                                                review_text TEXT,
                                                FOREIGN KEY(restaurant_id) REFERENCES restaurant(id),
                                                FOREIGN KEY(reviewer_id) REFERENCES reviewer(id),
                                                FOREIGN KEY(site_origin_id) REFERENCES site_origin(id)
                                                )
                                        """
        open_table_catagory_rating =    """
                                        CREATE TABLE IF NOT EXISTS open_table_catagory_rating(
                                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                                            reviewer_id INTEGER,
                                            review_id INTEGER,
                                            food INTEGER,
                                            ambience INTEGER,
                                            service INTEGER,
                                            FOREIGN KEY(reviewer_id) REFERENCES reviewer(id),
                                            FOREIGN KEY(review_id) REFERENCES restaurant_review(id)
                                        )
                                        """
        self.cur.execute(create_site_origin_table)
        self.cur.execute(create_restaurant_table)
        self.cur.execute(create_reviewer_table)
        self.cur.execute(create_res_review_table)
        self.cur.execute(open_table_catagory_rating)
        self.connection.close()

    def update_restuarant_table(self, csv_file_name):
        """
        
        """
        # set path to data
        PATH_TO_CSV = str(self.HOME / "data" /"raw" / csv_file_name)

        # connect to db
        self.connect()

        # define inserter
        inserter =  """
                    INSERT INTO restaurant( name,
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

                # check if this restaurant is already in db
                self.cur.execute("SELECT id FROM restaurant WHERE name = ?", (name, ))
                restaurant_id = self.cur.fetchone()

                # if this id exists, the restaurant is in the db
                if restaurant_id is None:
    
                    # add data to the db
                    self.connection.execute(inserter, db_row)
                    self.connection.commit()

        # close the connection to the db
        self.connection.close()

    def update_site_origin_table(self, csv_file_name):
        """

        """
        # set path to file name
        PATH_TO_CSV = str(self.HOME / "data" / "raw" / csv_file_name)

        # connect to db
        self.connect()

        # define inserter
        inserter =  """
                    INSERT INTO site_origin(site_name) VALUES (?)
                    """
        
        # open csv
        with open(PATH_TO_CSV, 'r') as file:
            reader = csv.reader(file)
            next(reader)

            # iterate over the rows extracting the data
            for row in reader:

                # row container
                db_row = []

                # extract data
                site_name = row[10]

                # add data to container
                db_row.append(site_name)

                # check if the site is already in the db
                self.cur.execute("SELECT id FROM site_origin WHERE site_name = ?", (site_name, ))
                site_id = self.cur.fetchone()

                # if this id exists, the site is in the db
                if site_id is None:
                    self.connection.execute(inserter, db_row)
                    self.connection.commit()

        self.connection.close()

    def update_reviewer_table(self, csv_file_name):

        # set path csv
        PATH_TO_CSV = self.HOME / "data" / "raw" / csv_file_name

        # connect to db
        self.connect()

        # define inserter
        inserter = """
                   INSERT INTO reviewer(name, 
                                        hometown) VALUES(?,?)
                   """
        
        # open csv
        with open(PATH_TO_CSV, "r") as file:
            reader = csv.reader(file)
            next(reader)

            for row in reader:

                db_row = []
                name = row[8].strip()
                hometown = row[6].strip().lower()

                db_row.extend([name, hometown])

                self.cur.execute("SELECT id FROM reviewer WHERE name = ?", (name, ))
                reviewer_id = self.cur.fetchone()

                if reviewer_id is None:
                    self.connection.execute(inserter, db_row)
                    self.connection.commit()

        self.connection.close()


    def update_restaurant_review_table(self, csv_file_name):

        # set path to csv
        PATH_TO_CSV = self.HOME / "data" / "raw" / csv_file_name

        # connect to db
        self.connect()

        # define inserter
        inserter = """
                   INSERT INTO res_review(restaurant_id,
                                          reviewer_id,
                                          site_origin_id,
                                          rating,
                                          date,
                                          review_text) VALUES(?,?,?,?,?,?)
                   """
        
        # open csv
        with open(PATH_TO_CSV, "r") as file:
            reader = csv.reader(file)
            next(reader)

            # iterate over row of the csv
            for row in reader:
                
                # data container
                db_row = []

                # get the reviwer name, res name, and site origin
                reviewer_name = row[8].strip()
                restaurant_name = row[9]
                site_origin = row[10]

                # get the corresponding ids
                self.cur.execute("SELECT id FROM reviewer WHERE name = ?", (reviewer_name, ))
                reviewer_id = self.cur.fetchone()
                reviewer_id = reviewer_id[0]

                self.cur.execute("SELECT id FROM restaurant WHERE name = ?", (restaurant_name, ))
                restaurant_id = self.cur.fetchone()
                restaurant_id = restaurant_id[0]

                self.cur.execute("SELECT id FROM site_origin WHERE site_name = ?", (site_origin, ))
                site_origin_id = self.cur.fetchone()
                site_origin_id = site_origin_id[0]

                # get data
                rating = row[1]
                date = row[7]
                text = row[5]

                # update container
                db_row.extend([restaurant_id, reviewer_id, site_origin_id, rating, date, text])

                self.connection.execute(inserter, db_row)
                self.connection.commit()

        self.connection.close
    
    def query(self, query):
        self.connect()
        self.cur.execute(query)
        result = self.cur.fetchall()

        return result





db_file_name = "restaurant_review_database.db"
restaurant_csv = "open_table_restaurant_data_Portland_ME_2024-05-28.csv"
review_csv = "open_table_review_data_Portland_ME_2024-05-28.csv"


ResDB = RestaurantReviewDB(db_file_name)
ResDB.connect()
ResDB.create_tables()
ResDB.update_restuarant_table(restaurant_csv)
ResDB.update_site_origin_table(review_csv)
ResDB.update_reviewer_table(review_csv)
ResDB.update_restaurant_review_table(review_csv)

query = """
            SELECT 
                reviewer.name,
                reviewer.hometown
            FROM
                reviewer
            JOIN 
                res_review ON reviewer.id = res_review.reviewer_id
            JOIN
                restaurant ON res_review.restaurant_id = restaurant.id
            WHERE
                restaurant.name = "David's 388"
        """
x = ResDB.query(query)

print(len(x))








        