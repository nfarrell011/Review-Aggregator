"""
Review Aggregator
Joseph Nelson Farrell
07-29-24

OpenTable Data Review Transformer Class

This file contains OpenTableReviewDataTransformer class. This class is used to transform the raw extracted review 
data from the OpenTable website to a form that is ready to be loaded in the restaurant_review_database.
"""
###################################################################################################################
# libraries
import pandas as pd  
import numpy as np  
from datetime import date, datetime, timedelta 
from pathlib import Path
import re
import datetime

###################################################################################################################
# class
class OpenTableReviewDataTransformer:
    """
    Class for transforming raw extracted OpenTable review data to curated data ready to be entered into the 
    restaurant_review_database.
    """
    def __init__(self) -> None:
        """
        Initializes the data transformer object. 
        """
        self.HOME = Path.cwd()
        self.raw_data = None
        self.file_name = None
        self.restaurants_to_drop_list = None

    def set_file_name(self, file_name:str) -> None:
        """
        Retrieves the file name for the raw data csv.
        """
        self.file_name = file_name
        return None

    def set_restaurants_to_drop(self, restaurants_to_drop_list:list) -> None:
        """
        Retrieves the restaurants to drop list.
        """
        self.restaurants_to_drop_list = restaurants_to_drop_list
        return None

    def set_data(self) -> None:
        """
        Retrieves the raw data from the csv file.
        """
        PATH_TO_DATA_FOLDER = self.HOME / "data" / "raw"
        PATH_TO_OPENTABLE_DATA = PATH_TO_DATA_FOLDER / self.file_name
        self.raw_data = pd.read_csv(PATH_TO_OPENTABLE_DATA)
        return None
    
    def clean_restaurant_name_columns(self, columns:list) -> None:
        """
        Cleans the two name columns, "restaurant_name_input" and "restaurant_name_extracted", preparing them to be
        compared. 
        
        This comparison is part of the data validation process and prevents erroneous data extractions (incorrect restaurants).

        Parameters:
        - columns: (list) - A list containing the two column names.
        
        Returns:
        - None
        """
        for col in columns:

            # string replacements
            self.raw_data[col] = self.raw_data[col].str.replace("&amp;", "and")
            self.raw_data[col] = self.raw_data[col].str.replace("&", "and")

            # removes "the" from restaurant names
            self.raw_data[col] = self.raw_data[col].str.replace(r'^\s*the\s+', '', case = False, regex = True)

            # make all letteres lowercase
            self.raw_data[col] = self.raw_data[col].str.lower()
        return None
    
    def encoder_fixer(self, text:str) -> str:
        """
        The extraction process assumed the website data was in "latin1", but it was in fact "utf-8" leading to data corruption issues.
        This will change the encoding and correct the issue.

        Parameters:
        - text: (str) - The text from the "description" column of the raw_data dataframe.

        Returns:
        - text: (str) - The text from the "description" column of the raw_data dataframe.
        """
        try:
            return text.encode('latin1').decode('utf-8')
        except (UnicodeEncodeError, UnicodeDecodeError):
            return text
        
    def fix_review_text_encoding(self) -> None:
        """
        Applies encoder_fixer to the elements of raw_data dataframe.
    
        Parameters:
        - None

        Returns:
        - None
        """
        self.raw_data["review_text"] = self.raw_data["review_text"].apply(self.encoder_fixer)
        return None
    
    def remove_erroneous_restaurant_reviews(self, restaurant_list: list) -> None:
        """
        Removes all reviews for restaurants identified as "incorrect" by the OpenTableResDataTransformer.
        Currently, this list of restaurant must be entered as a parameter

        Parameters:
        - restaurant_list: (list) - Restaurants whose reviews will be removed from dataframe.

        Returns:
        - None
        """
        for res in restaurant_list:
            self.raw_data = self.raw_data[self.raw_data["restaurant_name_input"] != res]
        return None
    
    def get_date_from_file_name(self) -> datetime.datetime:
        """
        Extracted the date from the file name. This can be used to convert some of the "datelike"
        elements in the form, "Dined 2 days ago", "Dined 3 days ago" etc., to actual dates.

        Parameters:
        - None

        Returns:
        - date: (datetime.datetime) - The date the data was extracted.
        """
        # regex used to extract the date from a string
        regex = r'ME_(\d{4}-\d{2}-\d{2})'

        # perfrom search
        match = re.findall(regex, self.file_name)
        date_str = match[0]

        # convert to datetime dtype
        date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
        return date

    def modify_date(self, text) -> None:
        """
        Converts a "datelike" element to an actual date. Uses regex and flags to determine how to perform the
        conversion.

        Parameters:
        - text: (str) - The datelike text extracted from the OpenTable

        Returns:
        - review_date: (datetime.datetime) - The date the review was left on.
        """
        # get the date of data extraction
        extraction_date = self.get_date_from_file_name()

        # covert the datelike element based on text in the element
        if "today" in text:
            review_date = extraction_date.date()

        elif "ago" in text:
            regex = r'Dined (\d+)'
            match = re.findall(regex, text)
            num_days_ago = int(match[0])
            review_date = (extraction_date - timedelta(days = num_days_ago)).date()
        else:
            regex = r"on (.+)"
            match = re.findall(regex, text)
            date_str = match[0]
            review_date = datetime.datetime.strptime(date_str, "%B %d, %Y").date()

        return review_date
    
    def update_datelike_column(self) -> None:
        """
        Applies modify date to a pd.DataFrame

        """
        self.raw_data["datelike"] = self.raw_data["datelike"].apply(lambda text: self.modify_date(text))
        return None

    def rename_columns(self) -> None:
        """
        Renames some of the dataframe columns.

        *** This will need to be updated to conform to scarper updates ***

        Parameters:
        - None

        Returns:
        - None

        """
        self.raw_data.rename(columns = {"restaurant_name_extracted": "reviewer_name",
                                        "restaurant_name_input": "restaurant_name",
                                        "Overall": "overall",
                                        "Food": "food",
                                        "Service": "service",
                                        "Ambience": "ambience"}, inplace = True)
        return None
            
    def drop_and_reorder_cols(self) -> None:
        """
        Drops columns: "restuarant_name_extracted" and "Unnamed: 0", renames "restaurant_name_input" "restaurant_name" and
        reorders the columns to facilitate database loading.

        Parameters:
        - None

        Returns:
        - None
        """
        self.raw_data.drop(["res_name", "Unnamed: 0"], axis = 1, inplace = True)
        column_order = ["restaurant_name", "datelike", "reviewer_name", "hometown", "overall", "food", "service", "ambience", "review_text", "origins"]
        self.raw_data = self.raw_data[column_order]
        return None
#################################################################################################################################
if __name__ == "__main__":
    pass
