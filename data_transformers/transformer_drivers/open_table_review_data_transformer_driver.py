"""
Review Aggregator
Joseph Nelson Farrell
07-29-24

OpenTable Review Data Transformer Driver

This file contains a driver for OpenTableReviewDataTransformer class. It will transform the OpenTable raw review data and 
stage the transformed data as a csv in the curated folder.
"""
###################################################################################################################
# libraries
import sys
import os
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from data_transformers.transformer_classes.open_table_review_data_transformer import OpenTableReviewDataTransformer

###################################################################################################################
# main
def main():
    """
    Transform the raw review data and stage it in a csv.
    """
    # parameters
    raw_data_file_name = "open_table_review_data_Portland_ME_2024-07-21.csv"
    restaurants_to_remove = ["continental", "low key"]

    data_transformer = OpenTableReviewDataTransformer()
    data_transformer.set_file_name(raw_data_file_name)
    data_transformer.set_restaurants_to_drop(restaurants_to_remove)
    data_transformer.set_data()
    data_transformer.clean_restaurant_name_columns(["restaurant_name_input"])
    data_transformer.fix_review_text_encoding()
    data_transformer.remove_erroneous_restaurant_reviews(restaurants_to_remove)
    data_transformer.update_datelike_column()
    data_transformer.rename_columns()
    data_transformer.clean_hometown_column()
    data_transformer.drop_and_reorder_cols()
    
    # save curated df as cvs
    SAVE_PATH = data_transformer.HOME / "data" / "curated" / f"{raw_data_file_name}_CURATED.csv"
    print(SAVE_PATH)
    data_transformer.raw_data.to_csv(str(SAVE_PATH))
    
if __name__ == "__main__":
    main()