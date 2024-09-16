"""
Review Aggregator
Joseph Nelson Farrell
07-29-24

Yelp Restaurant Data Transformer Class

This file contains YelpResDataTransformer class. This class is used to transform the raw extracted restuarant 
data from the Yelp website to a form that is ready to be loaded in the restaurant_review_database.
"""
#################################################################################################################################
# libraries
#################################################################################################################################
import pandas as pd  
from pathlib import Path
import re
import ast

#################################################################################################################################
# class
#################################################################################################################################
class YelpResDataTransformer:
    """
    Class for transforming raw extracted Yelp data to curated data ready to be entered into the 
    restaurant_review_database.
    """
    def __init__(self) -> None:
        """
        Initializes the data transformer object.

        Parameters:
        - None

        Returns:
        - None
        """
        self.HOME = Path.cwd()
        self.raw_data = None
        self.file_name = None

    def set_file_name(self, file_name:str) -> None:
        """
        Retrieves the file name for the raw data csv.

        Parameters:
        - file_name: (str) - The file name of the Yelp raw data.

        Returns:
        - None
        """
        self.file_name = file_name
        return self

    def set_data(self) -> None:
        """
        Retrieves the raw data from the csv file.

        Parameters:
        - None

        Returns:
        - None
        """
        try:
            PATH_TO_DATA_FOLDER = self.HOME / "data" / "raw"
            PATH_TO_OPENTABLE_DATA = PATH_TO_DATA_FOLDER / self.file_name
            self.raw_data = pd.read_csv(PATH_TO_OPENTABLE_DATA)
        except Exception as e:
            print(f"Error reading in data: {e}")

        return self
    
    def clean_restaurant_name_column(self) -> None:
        """
        Cleans the two name columns, "restaurant_name_input" and "restaurant_name_extracted", preparing them to be
        compared. 
        
        This comparison is part of the data validation process and prevents erroneous data extractions (incorrect restaurants).

        Parameters:
        - columns: (list) - A list containing the two column names.
        
        Returns:
        - None
        """
        try:
            # string replacements
            self.raw_data["name"] = self.raw_data["name"].str.replace("&amp;", "and")
            self.raw_data["name"] = self.raw_data["name"].str.replace("&", "and")

            # removes "the" from restaurant names
            self.raw_data["name"] = self.raw_data["name"].str.replace(r'^\s*the\s+', '', case = False, regex = True)

            # make all letteres lowercase
            self.raw_data["name"] = self.raw_data["name"].str.lower()

        except Exception as e:
            print(f"Error cleaning restaurant_name column: {e}")

        return self
                
    def clean_price_point_col(self) -> None:
        """
        Convert extractions that do not contain "$" to None.
        Conert symbols to numeric values.

        Parameters:
        - None

        Returns:
        - None
        """
        try:
            self.raw_data["price_point"] = self.raw_data["price_point"].apply(lambda x: None if (pd.isna(x)) or ("$" not in x) else x)
            mapper = {None:None,
                      "$":1,
                      "$$":2,
                      "$$$":3,
                      "$$$$":4}
            self.raw_data["price_point"] = self.raw_data["price_point"].map(mapper)
        except Exception as e:
            print(f"Error cleaning price_point column: {e}")

        return self
    
    def update_tag_col(self) -> None:
        """
        The tags columns contains strings literals that should be lists. It also contains NaN values.
        This function will convert the empty lists and NaN values to None, and convert the string literals to Python lists.

        * "[]"                              --> None
        * NaN                               --> None
        * "['string_1', 'string_2', ...]"   --> to list dtype

        Parameters:
        - None
        
        Returns:
        - None
        """
        try:
            self.raw_data["tags"] = self.raw_data["tags"].apply(lambda x: None if (x == "[]") or (pd.isna(x)) else x)
            self.raw_data["tags"] = self.raw_data["tags"].apply(lambda x: ast.literal_eval(x) if x is not None else x)
        except Exception as e:
            print(f"Error updating tag column: {e}")

        return self
    
    def add_city_and_state_columns(self) -> None:
        """
        Adds a city and state column by extracting the information from the column name. OpenTableScrapper generates
        "region" column and then transforms that column. One of these methods will be modified so that they match.
        """
        parts = self.file_name.split("_")
        city = parts[3]
        state = parts[4]
        self.raw_data["city"] = city
        self.raw_data["state"] = state
        return self

    def drop_rename_reorder_cols(self) -> None:
        """
        Drops columns: "restuarant_name_extracted" and "Unnamed: 0", renames "restaurant_name_input" "restaurant_name" and
        reorders the columns to facilitate database loading.

        Parameters:
        - None

        Returns:
        - None
        """
        try:
            self.raw_data.rename(columns = {"name": "restaurant_name"}, inplace = True)
            self.raw_data.drop(["Unnamed: 0"], axis = 1, inplace = True)

            column_order = ["restaurant_name", "city", "state", "price_point", "tags"]
            self.raw_data = self.raw_data[column_order]
        except Exception as e:
            print(f"Error dropping, renaming, and reordering columns: {e}")

        return self
    
    def save_transformed_data(self) -> None:
        """
        Saves transformed data to: data/curated/ folder
        """
        try:
            file_name = self.file_name.replace(".csv", "")
            SAVE_PATH = str(self.HOME / "data" / "curated" / f"{file_name}_CURATED.csv")
            self.raw_data.to_csv(SAVE_PATH)
        except Exception as e:
            print(f"Error saving data to csv: {e}")
        
        return self
    
    def execute(self, file_name:str) -> None:
        """
        Executes entire data transformation.

        Parameters:
        - file_name: (str) - file name of Yelp restaurant data.

        Returns:
        - None
        """
        try:
            (self
            .set_file_name(file_name)
            .set_data()
            .clean_restaurant_name_column()
            .clean_price_point_col()
            .update_tag_col()
            .add_city_and_state_columns()
            .drop_rename_reorder_cols()
            .save_transformed_data()
            )
        except Exception as e:
            print(f"Error executing transformation: {e}")
            
        return None
    
#################################################################################################################################
# End
#################################################################################################################################
if __name__ == "__main__":
    pass