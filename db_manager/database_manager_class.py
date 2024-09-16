"""
Review Aggregator
Joseph Nelson Farrell
07-29-24

Restaurant Review Data Base Class

This file contains RestaurantReviewDB class. This class manages the restaurant review database. It creates the database,
inserts the date, and allows querying.
"""
#################################################################################################################################
# Packages
#################################################################################################################################
import sqlite3
from pathlib import Path
import os
import csv
import ast
import pandas as pd

#################################################################################################################################
# Class
#################################################################################################################################
class RestaurantReviewDB:
    """
    A class for loading and interacting with the Restaurant Review Database. Inserting data is designed
    to be done after data has been extracted from OpenTable and Yelp and processed. The 4 resultant curated 
    data csv's serve as the attributes when inserting the data. Interactions between data files requires the
    data to be loaded simultaneously.

    Attributes:
     * HOME: (Path object)                       - The current working directory. Directory should be set to "review_aggregator".
     * file: (str)                               - The database file name.
     * connection: (SQLite Object)               - The connection to the database.
     * cur: (SQLite Object)                      - The database cursor.
     * yelp_restaurant_data: (Path Object)       - The path to the Yelp restaurant data. Set with string file name using
                                                   using setter method.
     * open_table_restaurant_data: (Path Object) - The path to the OpenTable restaurant data. Set with string file name using
                                                   using setter method.
     * yelp_review_data: (Path Object)           - The path to the Yelp review data. Set with string file name using
                                                   using setter method.
     * open_table_review_data: (Path Object)     - The path to the OpenTable review data. Set with string file name using
                                                   using setter method.

    Methods:
     * set_yelp_data
     * set_open_table_data
     * connect
     * create_tables
     * load_site_origin_table
     * load_region_table
     * load_tags_table
     * load_price_point_table
     * load_restaurant_table
     * load_res_tags_table
     * load_reviewer_table
     * load_restaurant_review_table
     * load_aux_rating_table
     * query
    """

    def __init__(self, db_file_name) -> None:
        """
        Initializer for RestaurantReviewDB

        Params:
         * db_file_name: (str) - The database file name.
        """
        self.HOME = Path.cwd().parent
        self.file_name = db_file_name
        self.connection = None
        self.cur = None
        self.yelp_restaurant_data = None
        self.yelp_review_data = None
        self.open_table_restaurant_data = None
        self.open_table_review_data = None

    def set_yelp_data(self, review_data_file_name:str, restaurant_data_file_name) -> None:
        """
        Sets both Yelp data attributes. 

        Params:
         * review_data_file_name: (str)     - File name of review data csv (CURATED).
         * restaurant_data_file_name: (str) - File name of restaurant data csv (CURATED).

        """
        CURATED_FOLDER_PATH = self.HOME / "data" / "curated"
        self.yelp_restaurant_data = CURATED_FOLDER_PATH / restaurant_data_file_name
        self.yelp_review_data = CURATED_FOLDER_PATH / review_data_file_name
        return self
    
    def set_open_table_data(self, review_data_file_name:str, restaurant_data_file_name) -> None:
        """
        Sets both OpenTable data attributes. 

        Params:
         * review_data_file_name: (str)     - File name of review data csv (CURATED).
         * restaurant_data_file_name: (str) - File name of restaurant data csv (CURATED).

        """
        CURATED_FOLDER_PATH = self.HOME / "data" / "curated"
        self.open_table_restaurant_data = CURATED_FOLDER_PATH / restaurant_data_file_name
        self.open_table_review_data = CURATED_FOLDER_PATH / review_data_file_name
        return self
    
    def connect(self) -> None:
        """
        Created connection to database. Sets connection and cur attributes.
        """
        # set the path to the db folder
        DB_FOLDER_PATH = self.HOME / "data" / "database"

        # check if the folder exists, if not create
        os.makedirs(DB_FOLDER_PATH, exist_ok = True)

        # set path to db file
        DB_FILE_PATH = DB_FOLDER_PATH / self.file_name

        # connect to db
        self.connection = sqlite3.connect(str(DB_FILE_PATH))
        self.cur = self.connection.cursor()
        return self

    def create_tables(self) -> None:
        """
        Creates database tables, if they do not exist. This is where the structure of the database can be modified.
        """
        # connect to db
        self.connect()

        # define the tables
        create_site_origin_table    =   """
                                        CREATE TABLE IF NOT EXISTS site_origin(
                                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                site_name TEXT NOT NULL UNIQUE
                                                )
                                        """
        create_region_table         =   """
                                        CREATE TABLE IF NOT EXISTS region(
                                                city TEXT,
                                                state TEXT,
                                                PRIMARY KEY (city, state)
                                                )
                                        """
        create_tags_table           =   """
                                        CREATE TABLE IF NOT EXISTS tag(
                                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                name TEXT NOT NULL UNIQUE
                                                )
                                        """
        create_price_point_table    =   """
                                        CREATE TABLE IF NOT EXISTS price_point(
                                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                price_point TEXT NOT NULL UNIQUE
                                                )
                                        """
        create_restaurant_table =       """
                                        CREATE TABLE IF NOT EXISTS restaurant(
                                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                name TEXT NOT NULL,
                                                price_point_id INTEGER,
                                                cuisine TEXT,
                                                description TEXT,
                                                city TEXT,
                                                state TEXT,
                                                FOREIGN KEY(city, state) REFERENCES region(city, state),
                                                FOREIGN KEY(price_point_id) REFERENCES price_point(id)
                                                )
                                        """
        create_res_tags_table   =       """
                                        CREATE TABLE IF NOT EXISTS restaurant_tag(
                                                restaurant_id INTEGER,
                                                tag_id INTEGER,
                                                PRIMARY KEY (restaurant_id, tag_id),
                                                FOREIGN KEY(tag_id) REFERENCES tag(id),
                                                FOREIGN KEY (restaurant_id) REFERENCES restaurant(id)
                                                )
                                        """
        create_reviewer_table       =   """
                                        CREATE TABLE IF NOT EXISTS reviewer(
                                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                name TEXT NOT NULL,
                                                hometown TEXT
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
                                        CREATE TABLE IF NOT EXISTS open_table_category_rating(
                                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                reviewer_id INTEGER,
                                                review_id INTEGER,
                                                food INTEGER,
                                                ambience INTEGER,
                                                service INTEGER,
                                                FOREIGN KEY(reviewer_id) REFERENCES reviewer(id),
                                                FOREIGN KEY(review_id) REFERENCES res_review(id)
                                                )
                                        """
        tables_list = [create_tags_table, create_price_point_table, create_region_table, create_res_review_table,
                       create_restaurant_table, create_reviewer_table, create_site_origin_table, 
                       open_table_catagory_rating, create_res_tags_table]
        
        for table in tables_list:
            self.cur.execute(table)
        self.connection.close()
        return self
    
    def load_site_origin_table(self) -> None:
        """
        Inserts data into the site origin table.
        """
        # Path to data source
        PATH_TO_YELP_CSV = str(self.yelp_review_data)
        PATH_TO_OPEN_TABLE_CSV = str(self.open_table_review_data)

        csv_list = [PATH_TO_OPEN_TABLE_CSV, PATH_TO_YELP_CSV]
        
        try: 
            # Connect to db
            self.connect()

            # Define inserter
            inserter =  """
                        INSERT OR IGNORE INTO site_origin( site_name ) VALUES (?)
                        """

            # Iterate over data in both cvs, loading data
            for csv_file in csv_list:
                with open(csv_file, "r") as file:
                    reader = csv.reader(file)
                    next(reader)

                    # Iterate over each row in the csv
                    for row in reader:

                        # Container for data to load
                        db_row = []

                        # Extracts the last element from the row
                        site_origin = row[-1]

                        # Update the data containter
                        db_row.append(site_origin)

                        # Insert data
                        self.connection.execute(inserter, db_row)
            
            # Commit changes to db
            self.connection.commit()

        except FileNotFoundError as e:
            print(f"Error: File not found - {e}")
        except sqlite3.Error as e:
            print(f"Database error: {e}")
        finally:
            self.connection.close()
        return self

    def load_region_table(self) -> None:
        """
        Inserts data into the region table.
        """
        # Path to data source
        PATH_TO_YELP_CSV = str(self.yelp_restaurant_data)
        PATH_TO_OPEN_TABLE_CSV = str(self.open_table_restaurant_data)

        csv_list = [PATH_TO_OPEN_TABLE_CSV, PATH_TO_YELP_CSV]
        
        try: 
            # Connect to db
            self.connect()

            # Define inserter
            inserter =  """
                        INSERT OR IGNORE INTO region( city, state ) VALUES (?,?)
                        """

            # Iterate over data in both cvs, loading data
            for csv_file in csv_list:
                with open(csv_file, "r") as file:
                    reader = csv.reader(file)
                    next(reader)

                    # Iterate over each row in the csv
                    for row in reader:

                        # Container for data to load
                        db_row = []

                        # Extracts the last element from the row
                        city = row[2]
                        state = row[3].strip() ### FIX THIS IN TRANSFORMER

                        # Update the data containter
                        db_row.extend([city, state])

                        # Insert data
                        self.connection.execute(inserter, db_row)
            
            # Commit changes to db
            self.connection.commit()

        except FileNotFoundError as e:
            print(f"Error: File not found - {e}")
        except sqlite3.Error as e:
            print(f"Database error: {e}")
        finally:
            self.connection.close()
        return self
    
    def load_tags_table(self) -> None:
        """
        Inserts data into the tag table.
        """
        # Path to data source
        PATH_TO_YELP_CSV = str(self.yelp_restaurant_data)
        PATH_TO_OPEN_TABLE_CSV = str(self.open_table_restaurant_data)

        csv_list = [PATH_TO_OPEN_TABLE_CSV, PATH_TO_YELP_CSV]
        
        try: 
            # Connect to db
            self.connect()

            # Define inserter
            inserter =  """
                        INSERT OR IGNORE INTO tag( name ) VALUES (?)
                        """

            # Iterate over data in both cvs, loading data
            for csv_file in csv_list:
                with open(csv_file, "r") as file:
                    reader = csv.reader(file)
                    next(reader)

                    # Iterate over each row in the csv
                    for row in reader:

                        # Extracts the last element from the row
                        tags = row[-1]

                        # If it's an empty string, literal_eval will fail so skip.
                        if tags == "":
                            continue

                        # Converts string literals to dtypes; here lists
                        tags = ast.literal_eval(tags)

                        # Iterate over the tags in the list
                        for tag in tags:

                            # Container for data to load
                            db_row = [tag]

                            # Insert data
                            self.connection.execute(inserter, db_row)
            
            # Commit changes to db
            self.connection.commit()

        except FileNotFoundError as e:
            print(f"Error: File not found - {e}")
        except sqlite3.Error as e:
            print(f"Database error: {e}")
        finally:
            self.connection.close()
        return self
    
    def load_price_point_table(self) -> None:
        """
        Inserts data into the price_point table.
        """
        # Path to data source
        csv_file = str(self.yelp_restaurant_data)
        
        try: 
            # Connect to db
            self.connect()

            # Define inserter
            inserter =  """
                        INSERT OR IGNORE INTO price_point( price_point ) VALUES (?)
                        """
            # Iterate over data in both cvs, loading data
            with open(csv_file, "r") as file:
                reader = csv.reader(file)
                next(reader)

                # Iterate over each row in the csv
                for row in reader:
                    
                    # Extract price_point
                    price_point = row[4]

                    # Skip if the price_point is empty string
                    if price_point == "":
                        continue

                    # Container for data to load
                    db_row = [price_point]

                    # Insert data
                    self.connection.execute(inserter, db_row)
        
            # Commit changes to db
            self.connection.commit()
                
        except FileNotFoundError as e:
            print(f"Error: File not found - {e}")
        except sqlite3.Error as e:
            print(f"Database error: {e}")
        finally:
            self.connection.close()
        return self

    def load_restuarant_table(self):
        """
        Inserts data into the restaurant table.
        """
        # Path to data source
        PATH_TO_OPEN_TABLE_CSV = str(self.open_table_restaurant_data)
        PATH_TO_YELP_CSV = str(self.yelp_restaurant_data)        
        try: 
            # Connect to db
            self.connect()

            # Define inserter
            inserter =  """
                        INSERT OR IGNORE INTO restaurant( name,
                                                          price_point_id,
                                                          cuisine,
                                                          description,
                                                          city,
                                                          state) VALUES (?,?,?,?,?,?)
                        """

            with open(PATH_TO_YELP_CSV, "r") as yelp_file:
                yelp_reader = csv.reader(yelp_file)
                next(yelp_reader)

                # Iterate over each row in the csv
                for yelp_row in yelp_reader:

                    # Flag for setting cuisine and description to none
                    got_cuisine_and_description_flag = False
                    
                    # Container for data to load
                    db_row = []

                    # Extract data
                    name = yelp_row[1].strip() # restaurant name
                    price_point = yelp_row[4] # price_point
                    city = yelp_row[2].strip()
                    state = yelp_row[3].strip()

                    # Verify city, state are in region table
                    self.cur.execute("SELECT 1 FROM region WHERE city = ? AND state = ?", (city, state))
                    region_exists = self.cur.fetchone()

                    # If not, insert city, state
                    if not region_exists:
                        self.cur.execute("INSERT INTO region (city, state) VALUES (?, ?)", (city, state))
                        print(f"Inserted new region: {city}, {state}")

                    # Get the corresponding id for price point, if there is one
                    if price_point == "":
                        price_point = None
                    
                    else:
                        self.cur.execute("SELECT id FROM price_point WHERE price_point = ?", (price_point, ))
                        price_point_id = self.cur.fetchone()
                        price_point_id = price_point_id[0]

                    with open(PATH_TO_OPEN_TABLE_CSV, "r") as open_table_file:
                        open_table_reader = csv.reader(open_table_file)
                        next(open_table_reader)
                        for open_table_row in open_table_reader:
                            if open_table_row[1] == name:
                                cuisine = open_table_row[4]
                                description = open_table_row[5]
                                got_cuisine_and_description_flag = True
                                break
                    
                    # Update the data containter
                    if not got_cuisine_and_description_flag:
                        cuisine = None
                        description = None

                    # Update data to load    
                    db_row.extend([name, price_point, cuisine, description, city, state])
                    
                    # Insert data
                    self.connection.execute(inserter, db_row)

                    # Update flag
                    got_cuisine_and_description_flag = False
        
            # Commit changes to db
            self.connection.commit()

        except FileNotFoundError as e:
            print(f"Error: File not found - {e}")
        except sqlite3.Error as e:
            print(f"Database error: {e}")
        finally:
            self.connection.close()
        return self
    
    def load_res_tags_table(self):
        """
        Inserts data into the restaurant_tag table.
        """
        # Path to data source
        PATH_TO_OPEN_TABLE_CSV = str(self.open_table_restaurant_data)
        PATH_TO_YELP_CSV = str(self.yelp_restaurant_data)        
        try: 
            # Connect to db
            self.connect()

            # Define inserter
            inserter =  """
                        INSERT OR IGNORE INTO restaurant_tag( restaurant_id,
                                                              tag_id) VALUES (?,?)
                        """
            # First, open the Yelp csv and iterate over the restaurants extracting the tags 
            with open(PATH_TO_YELP_CSV, "r") as yelp_file:
                yelp_reader = csv.reader(yelp_file)
                next(yelp_reader)

                for yelp_row in yelp_reader:
                    restaurant_tags_list = []
                    yelp_name = yelp_row[1].strip()
                    yelp_tags = yelp_row[-1]

                    with open(PATH_TO_OPEN_TABLE_CSV, "r") as open_table_file:
                        open_table_reader = csv.reader(open_table_file)
                        next(open_table_reader)
                        
                        open_table_tags = None
                        for open_table_row in open_table_reader:
                            if yelp_name == open_table_row[1]:
                                open_table_tags = open_table_row[-1]
                                break
                            else:
                                continue
                    
                    # Process Yelp tags
                    load_yelp_flag = False
                    if yelp_tags:
                        try:
                            yelp_tags = ast.literal_eval(yelp_tags)
                            load_yelp_flag = True
                        except (SyntaxError, ValueError):
                            print(f"Error parsing tags for {yelp_name} from Yelp data")

                    # Process OpenTable tags
                    load_open_table_flag = False
                    if open_table_tags:
                        try:
                            open_table_tags = ast.literal_eval(open_table_tags)
                            load_open_table_flag = True
                        except (SyntaxError, ValueError):
                            print(f"Error parsing tags for {yelp_name} from OpenTable data")

                    # Combine tag lists, if they exist
                    if load_yelp_flag:
                        restaurant_tags_list.extend(yelp_tags)
                    if load_open_table_flag:
                        restaurant_tags_list.extend(open_table_tags)

                    # Insert res tags into the database if any exist
                    if restaurant_tags_list:
                        for tag in set(restaurant_tags_list):

                            # Extract the tag_id from the tag lookup table
                            self.cur.execute("SELECT id FROM tag WHERE name = ?", (tag,))
                            tag_id = self.cur.fetchone()
                            if tag_id:
                                tag_id = tag_id[0]
                            else:
                                print(f"Tag '{tag}' not found in the tag lookup table.")
                                continue

                            # Extract the restaurant_id from the restaurant lookup table
                            self.cur.execute("SELECT id FROM restaurant WHERE name = ?", (yelp_name,))
                            name_id = self.cur.fetchone()
                            if name_id:
                                name_id = name_id[0]  # Extract the ID from the tuple
                            else:
                                print(f"Restaurant '{yelp_name}' not found in the restaurant lookup table.")
                                continue

                            # Insert the restaurant_id and tag_id into the restaurant_tag table
                            self.connection.execute(inserter, (name_id, tag_id))

            # Commit changes to db
            self.connection.commit()

        except FileNotFoundError as e:
            print(f"Error: File not found - {e}")
        except sqlite3.Error as e:
            print(f"Database error: {e}")
        finally:
            self.connection.close()
        return self

    def load_reviewer_table(self):
        """
        Inserts data into the reviewer table.
        """
        # Path to data source
        PATH_TO_YELP_CSV = str(self.yelp_review_data)
        PATH_TO_OPEN_TABLE_CSV = str(self.open_table_review_data)

        csv_list = [PATH_TO_OPEN_TABLE_CSV, PATH_TO_YELP_CSV]
        
        try: 
            # Connect to db
            self.connect()

            # Define inserter
            inserter =  """
                        INSERT INTO reviewer(name, hometown) VALUES (?, ?)
                        """

            # Iterate over data in both cvs, loading data
            for csv_file in csv_list:
                with open(csv_file, "r") as file:
                    reader = csv.reader(file)
                    next(reader)

                    # Iterate over each row in the csv
                    for row in reader:
                        
                        # Container for data to be loaded
                        db_row = []

                        # Extract data from csv row
                        name = row[3].strip()
                        if name == "":
                            break
                        hometown = row[4].strip()
                        if hometown == "":
                            hometown = None

                        # Insert data 
                        db_row.extend([name, hometown])
                        self.connection.execute(inserter, db_row)
            
            # Commit changes to db
            self.connection.commit()

        except FileNotFoundError as e:
            print(f"Error: File not found - {e}")
        except sqlite3.Error as e:
            print(f"Database error: {e}")
        finally:
            self.connection.close()
        return self

    def load_restaurant_review_table(self):
        """ 
        Inserts data into the restaurant_review table.
        """
        # Path to data source
        PATH_TO_YELP_CSV = str(self.yelp_review_data)
        PATH_TO_OPEN_TABLE_CSV = str(self.open_table_review_data)

        csv_list = [PATH_TO_OPEN_TABLE_CSV, PATH_TO_YELP_CSV]

        try: 
            # connect to db
            self.connect()

            # define inserter
            inserter = """
                    INSERT INTO res_review( restaurant_id,
                                            reviewer_id,
                                            site_origin_id,
                                            rating,
                                            date,
                                            review_text) VALUES(?,?,?,?,?,?)
                    """
            
            for index, csv_file in enumerate(csv_list):
                with open(csv_file, "r") as file:
                    reader = csv.reader(file)
                    next(reader)

                    # iterate over row of the csv
                    for row in reader:
                        
                        # data container
                        db_row = []

                        # The csv's contain different data, so this will make sure the correct data is loaded
                        if index == 0: # Yelp table
                            restaurant_name = row[1].strip()
                            date = row[2]
                            reviewer_name = row[3].strip()
                            rating = row[5]
                            text = row[9]
                            site_origin = row[-1]
                        else: # Open Table
                            restaurant_name = row[1].strip()
                            date = row[2]
                            reviewer_name = row[3].strip()
                            rating = row[7]
                            text = row[8]
                            site_origin = row[-1]

                        # Do not attempt load records with no name
                        if reviewer_name == "":
                            continue
                        
                        # Get the corresponding ids from lookup tables
                        self.cur.execute("SELECT id FROM reviewer WHERE name = ?", (reviewer_name, ))
                        reviewer_id = self.cur.fetchone()
                        if not reviewer_id:
                            continue
                        reviewer_id = reviewer_id[0]

                        self.cur.execute("SELECT id FROM restaurant WHERE name = ?", (restaurant_name, ))
                        restaurant_id = self.cur.fetchone()
                        if not restaurant_id:
                            continue
                        restaurant_id = restaurant_id[0]

                        self.cur.execute("SELECT id FROM site_origin WHERE site_name = ?", (site_origin, ))
                        site_origin_id = self.cur.fetchone()
                        site_origin_id = site_origin_id[0]

                        # update container
                        db_row.extend([restaurant_id, reviewer_id, site_origin_id, rating, date, text])

                        self.connection.execute(inserter, db_row)
                        self.connection.commit()

            # Commit changes to db
            self.connection.commit()

        except FileNotFoundError as e:
            print(f"Error: File not found - {e}")
        except sqlite3.Error as e:
            print(f"Database error: {e}")
        finally:
            self.connection.close()
        return self
    
    def load_aux_rating_table(self) -> None:
        """
        Inserts data into the open_table_category_rating table.
        """
        # Path to data source
        csv_file = str(self.open_table_review_data)
        
        try: 
            # Connect to db
            self.connect()

            # Define inserter
            inserter =  """
                        INSERT INTO open_table_category_rating( reviewer_id,
                                                                review_id,
                                                                food,
                                                                ambience,
                                                                service ) VALUES (?,?,?,?,?)
                        """
            # Iterate over data in both cvs, loading data
            with open(csv_file, "r") as file:
                reader = csv.reader(file)
                next(reader)

                # Iterate over each row in the csv
                for row in reader:

                    db_row = []
                    
                    # Extract price_point
                    restaurant_name = row[1].strip()
                    date = row[2]
                    reviewer_name = row[3]
                    food = row[6]
                    service = row[7]
                    ambience = row[8]

                    self.cur.execute("SELECT id FROM reviewer WHERE name = ?", (reviewer_name, ))
                    reviewer_id = self.cur.fetchone()
                    if not reviewer_id:
                        break
                    reviewer_id = reviewer_id[0]

                    self.cur.execute("SELECT id FROM restaurant WHERE name = ?", (restaurant_name, ))
                    restaurant_id = self.cur.fetchone()
                    restaurant_id = restaurant_id[0]

                    self.cur.execute("""
                                        SELECT id FROM res_review WHERE reviewer_id = ? AND 
                                                                        date = ? AND
                                                                        restaurant_id = ?
                                     """, (reviewer_id, date, restaurant_id))
                    review_id = self.cur.fetchone()
                    review_id = review_id[0]


                    # Container for data to load
                    db_row.extend([restaurant_id, review_id, food, ambience, service])

                    # Insert data
                    self.connection.execute(inserter, db_row)
        
            # Commit changes to db
            self.connection.commit()
                
        except FileNotFoundError as e:
            print(f"Error: File not found - {e}")
        except sqlite3.Error as e:
            print(f"Database error here: {e}")
        finally:
            self.connection.close()
        return self
    
    def query(self, query:str) -> pd.DataFrame:
        """ 
        Method used to extract data from the database using SQLite Syntax.

        Params:
         * query: (str) - A query written in SQL

        Returns:
         * result: (pd.DataFrame) - A dataframe containing the results of the query.
        """
        self.connect()
        self.cur.execute(query)
        result = self.cur.fetchall()
        col_names = [description[0] for description in self.cur.description]
        result = pd.DataFrame(result, columns = col_names)
        self.cur.close()
        self.connection.close()

        return result
#################################################################################################################################
# End
#################################################################################################################################
if __name__ == "__main__":
    pass









        