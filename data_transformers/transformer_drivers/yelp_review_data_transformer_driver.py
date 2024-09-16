"""
Review Aggregator
Joseph Nelson Farrell
07-29-24

Yelp Review Data Transformer Driver

This file contains a driver for YelpReviewDataTransformer class. It will transform the OpenTable raw data and stage the
transformed data as a csv in the curated folder.
"""
###################################################################################################################
# libraries
import sys
import os
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from data_transformers.transformer_classes.yelp_review_data_transformer import YelpReviewDataTransformer


###################################################################################################################
# main
def main():
    """
    Transform the raw restaurant data and stage it in a csv.
    """
    FILE_NAME = "yelp_review_data_Portland_ME_2024-06-29.csv"
    data_transformer = YelpReviewDataTransformer()
    data_transformer.execute(FILE_NAME)
    
if __name__ == "__main__":
    main()