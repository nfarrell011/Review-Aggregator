"""
Review Aggregator
Joseph Nelson Farrell
07-29-24

OpenTable Data Transformer Class

This file contains OpenTableResDataTransformer class. This class is used to transform the raw extracted restuarant 
data from the OpenTable website to a form that is ready to be loaded in the restaurant_review_database.
"""
###################################################################################################################
# libraries
import pandas as pd  
from pathlib import Path
import re
import ast

###################################################################################################################
# class
class OpenTableResDataTransformer:
    """
    Class for transforming raw extracted OpenTable data to curated data ready to be entered into the 
    restaurant_review_database.
    """
    def __init__(self) -> None:
        """
        Initializes the data transformer object. 
        """
        self.HOME = Path.cwd()
        self.raw_data = None
        self.file_name = None
        self.drop_list = None # restaurant removed from data
        self.restaurants_to_inspect_list = None # restaurants that require further validation

    def set_file_name(self, file_name:str) -> None:
        """
        Retrieves the file name for the raw data csv
        """
        self.file_name = file_name
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

    def get_non_matching_restaurant_name(self) -> pd.DataFrame:
        """
        Compares the two restuarant name columns and returns a dataframe containing the instances where the names 
        do not match.

        Parameters:
        - None

        Returns:
        - None
        """
        # check is the extrated name matches the input name
        non_matching_indices = self.raw_data["restaurant_name_extracted"] != self.raw_data["restaurant_name_input"]

        # filter the df for those indices
        non_matching_df = self.raw_data[non_matching_indices]
        return non_matching_df
    
    def check_for_substring(self, row:pd.Series) -> bool:
        """
        Checks if either restaurant name column is contained in the other column. This addresses instances where restaurant names differ
        accross various websites. The restaurants that produce a partial match currently require manual validation.

        Parameters:
        row: (pd.Series) - A row of the raw_data dataframe
        
        Returns:
        - (bool) - Indicating if the one column is a substring of the other column.
        """
        return (row["restaurant_name_extracted"] in row["restaurant_name_input"]) or (row["restaurant_name_input"] in row["restaurant_name_extracted"])
    
    def remove_inadvertent_extractions(self) -> None:   
        """
        Removes restaurants that were scraped inadvertently, i.e., the OpenTable search returned the incorrect restaurant. This validation uses name
        matching, i.e., input vs. extracted name. The function also generates a list of restaurants that require manual inspection, i.e., those that
        paritally match.

        Parameters:
        - None

        Returns:
        -None
        """
        # filter the dataframe for restaurant names that do not match
        non_matching_res_name_df = self.get_non_matching_restaurant_name()

        # create a boolean column, indicating a partial match
        non_matching_res_name_df["partial_match"] = non_matching_res_name_df.apply(lambda row: self.check_for_substring(row), axis = 1)

        # get indices that are not a complete or partial match; these will dropped
        drop_indices = non_matching_res_name_df[(non_matching_res_name_df["partial_match"] == False)].index.to_list()

        # generate a list of partial matches for further validation
        restaurant_to_check = non_matching_res_name_df.loc[non_matching_res_name_df["partial_match"] == True, "restaurant_name_input"]

        # create list of dropped restaurants
        dropped_res = self.raw_data.loc[drop_indices, "restaurant_name_input"].values.tolist()

        # update attributes
        self.raw_data = self.raw_data.drop(drop_indices)
        self.drop_list = dropped_res
        self.restaurants_to_inspect_list = restaurant_to_check
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
        
    def fix_description_encoding(self) -> None:
        """
        Applies encoder_fixer to the elements of raw_data dataframe.
    
        Parameters:
        - None

        Returns:
        - None
        """
        self.raw_data["description"] = self.raw_data["description"].apply(self.encoder_fixer)
        return None

    def seperate_region(self) -> None:
        """
        Seperates "region" into two columns, "city and "state".

        Parameters:
        - None

        Returns:
        - None
        """
        city_state_df = self.raw_data["region"].str.split(",", expand = True)
        city_state_df.columns = ["city", "state"]
        self.raw_data.drop("region", inplace = True, axis = 1)
        self.raw_data = pd.concat([self.raw_data, city_state_df], axis = 1)
        return None
    
    def extract_price_range(self, text:str) -> tuple:
        """
        Extracts the numeric portions of the price range string.

        Parameters:
        - text: (str) - The text from the "price_range" column of the raw_data dataframe.

        Returns:
        - (min, max): (tuple) - A tuple containing the min price (str) and max price (str)
        """
        # regular expression to extract two elements following "$"
        regex = r'\$(\d{2})'

        # identfies the different price_range displays
        if "to" in text:
            match = re.findall(regex, text)
            min = match[0]
            max = match[1]
            return min, max
        elif 'under' in text:
            match = re.findall(regex, text)
            min = 0
            max = match[0]
            return min, max
        else:
            match = re.findall(regex, text)
            min = match[0]
            max = 200
            return min, max
        
    def seperate_price_range_cols(self) -> None:
        """
        Seperates the "price_range" column into two columns: min_price, max_price

        Parameters:
        - None

        Returns:
        - None
        """
        self.raw_data[["min_price", "max_price"]] = self.raw_data["price_point"].apply(lambda x: pd.Series(self.extract_price_range(x)))
        self.raw_data.drop("price_point", axis = 1, inplace = True)
        self.raw_data["min_price"] = pd.to_numeric(self.raw_data["min_price"])
        self.raw_data["max_price"] = pd.to_numeric(self.raw_data["max_price"])
        return None
    
    def update_tag_cols(self) -> None:
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
        self.raw_data["tags"] = self.raw_data["tags"].apply(lambda x: None if (x == "[]") or (pd.isna(x)) else x)
        self.raw_data["tags"] = self.raw_data["tags"].apply(lambda x: ast.literal_eval(x) if x is not None else x)

    def drop_and_reorder_cols(self):
        """
        Drops columns: "restuarant_name_extracted" and "Unnamed: 0", renames "restaurant_name_input" "restaurant_name" and
        reorders the columns to facilitate database loading.

        Parameters:
        - None

        Returns:
        - None
        """
        self.raw_data.drop(["restaurant_name_extracted", "Unnamed: 0"], axis = 1, inplace = True)
        self.raw_data.rename(columns = {"restaurant_name_input": "restaurant_name"}, inplace = True)

        column_order = ["restaurant_name", "city", "state", "cuisine", "description", "min_price", "max_price", "tags"]
        self.raw_data = self.raw_data[column_order]

    def generate_summary(self):
        """ 
        Prints a summery of the transformation process: restaurants dropped, restaurants to inspect, and 
        prints count summaries.

        Parameters:
        - None

        Returns:
        - None
        """
        print()
        print("-" * 75)
        print(f"The number of restuarants dropped for data integrity concerns: {len(self.drop_list)}")
        for index, res in enumerate(self.drop_list):
            print(f"\t{index + 1}: {res}")
        print()
        print(f"The number restaurants that need further inspection: {len(self.restaurants_to_inspect_list)}")
        print()
        print("Manually inspect these restaurants for data integrity concerns:")
        for index, res in enumerate(self.restaurants_to_inspect_list):
            print(f"\t{index + 1}: {res}")
        print()
        print(f"Restaurants cleared and ready to be added the database: {len(self.raw_data) - len(self.restaurants_to_inspect_list)}")
        print("-" * 75)
        print(self.raw_data)

#################################################################################################################################
if __name__ == "__main__":
    pass




        