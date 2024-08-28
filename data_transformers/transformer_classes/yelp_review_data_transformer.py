"""
Review Aggregator
Joseph Nelson Farrell
07-29-24

Yelp Review Data Transformer Class

This file contains YelpReviewDataTransformer class. This class is used to transform the raw extracted restuarant 
data from the Yelp website to a form that is ready to be loaded in the restaurant_review_database.
"""
#################################################################################################################################
# libraries
#################################################################################################################################
import pandas as pd  
from pathlib import Path
import re
import ast
import datetime 

#################################################################################################################################
# class
#################################################################################################################################
class YelpReviewDataTransformer:
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
            self.raw_data["restaurant"] = self.raw_data["restaurant"].str.replace("&amp;", "and")
            self.raw_data["restaurant"] = self.raw_data["restaurant"].str.replace("&", "and")

            # removes "the" from restaurant names
            self.raw_data["restaurant"] = self.raw_data["restaurant"].str.replace(r'^\s*the\s+', '', case = False, regex = True)

            # make all letteres lowercase
            self.raw_data["restaurant"] = self.raw_data["restaurant"].str.lower()

        except Exception as e:
            print(f"Error cleaning restaurant_name column: {e}")

        return self
                
    def clean_datelike_col(self) -> None:
        """
        Converts "datelike" column elemnts to datetime.datetime object

        Parameters:
        - None

        Returns:
        - None
        """
        try:
            self.raw_data["datelike"] = self.raw_data["datelike"].apply(lambda x: datetime.datetime.strptime(x, "%b %d, %Y"))
        except Exception as e:
            print(f"Error cleaning price_point column: {e}")

        return self
    
    def get_rating_integer_from_text(self, text:str):
        """
        Extracts an integer from a string
        """
        regex = r'(\d+)'

        match = re.findall(regex, text)
        rating_str = match[0]
        rating = int(rating_str)
        return rating
    
    def clean_rating_column(self) -> None:
        """
        Extracts the integer portion of the text in the rating column

        Parameters:
        - None
        
        Returns:
        - None
        """
        try:
            self.raw_data["rating"] = self.raw_data["rating"].apply(lambda x: self.get_rating_integer_from_text(x))
        except Exception as e:
            print(f"Error updating tag column: {e}")

        return self

    def drop_rename_reorder_cols(self) -> None:
        """
        Drops columns: "Unnamed: 0", renames "restaurant" and "text" and reorders the columns to facilitate 
        database loading.

        Parameters:
        - None

        Returns:
        - None
        """
        try:
            self.raw_data.rename(columns = {"restaurant": "restaurant_name",
                                            "text": "review_text"}, inplace = True)
            self.raw_data.drop(["Unnamed: 0"], axis = 1, inplace = True)

            column_order = ["restaurant_name", "datelike", "reviewer_name", "hometown", "rating", "review_text", "origins"]
            self.raw_data = self.raw_data[column_order]
        except Exception as e:
            print(f"Error dropping, renaming, and reordering columns: {e}")

        return self
    
    def save_transformed_data(self):
        """
        Saves transformed data to: data/curated/ folder

        Parameters:
        - None

        Returns:
        - None
        """
        try:
            SAVE_PATH = str(self.HOME / "data" / "curated" / f"{self.file_name}_CURATED.csv")
            self.raw_data.to_csv(SAVE_PATH)
        except Exception as e:
            print(f"Error saving data to csv: {e}")
        
        return self
    
    def execute(self, file_name:str) -> None:
        """
        Executes entire data transformation.

        Parameters:
        - file_name: (str) - file name of Yelp review data data.

        Returns:
        - None
        """
        try:
            (self
            .set_file_name(file_name)
            .set_data()
            .clean_restaurant_name_column()
            .clean_datelike_col()
            .clean_rating_column()
            .drop_rename_reorder_cols()
            .save_transformed_data()
            )
        except Exception as e:
            print(f"Error executing transformation: {e}")
            
        return None

#################################################################################################################################
# End
#################################################################################################################################

file_name = "yelp_review_data_Portland_ME_2024-06-29.csv"

data_transformer = YelpReviewDataTransformer()
data_transformer.execute(file_name)
print(data_transformer.raw_data)

#if __name__ == "__main__":
#    pass