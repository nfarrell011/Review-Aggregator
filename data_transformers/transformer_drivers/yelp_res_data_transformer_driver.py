"""
Review Aggregator
Joseph Nelson Farrell
07-29-24

Yelp Restaurant Data Transformer Driver

This file contains a driver for YelpResDataTransformer class. It will transform the Yelp raw data and stage the
transformed data as a csv in the curated folder.
"""
###################################################################################################################
# libraries
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from data_transformers.transformer_classes.yelp_res_data_transformer import YelpResDataTransformer

###################################################################################################################
# main
def main():
    """
    Transform the raw restaurant data and stage it in a csv.
    """
    FILE_NAME = "yelp_restaurant_data_Portland_ME_2024-06-29.csv"
    data_transformer = YelpResDataTransformer()
    data_transformer.execute(FILE_NAME)
  
if __name__ == "__main__":
    main()

