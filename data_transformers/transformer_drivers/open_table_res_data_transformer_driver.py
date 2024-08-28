"""
Review Aggregator
Joseph Nelson Farrell
07-29-24

OpenTable Restaurant Data Transformer Driver

This file contains a driver for OpenTableResDataTransformer class. It will transform the OpenTable raw data and stage the
transformed data as a csv in the curated folder.
"""
###################################################################################################################
# libraries
import sys
import os
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from data_transformers.transformer_classes.open_table_res_data_transformer import OpenTableResDataTransformer


###################################################################################################################
# main
def main():
    """
    Transform the raw restaurant data and stage it in a csv.
    """
    raw_data_file_name = "open_table_restaurant_data_Portland_ME_2024-07-21.csv"
    data_transformer = OpenTableResDataTransformer()
    data_transformer.set_file_name(raw_data_file_name)
    data_transformer.set_data()
    data_transformer.clean_restaurant_name_columns(["restaurant_name_extracted", "restaurant_name_input"])
    data_transformer.remove_inadvertent_extractions()
    data_transformer.fix_description_encoding()
    data_transformer.seperate_region()
    data_transformer.seperate_price_range_cols()
    data_transformer.update_tag_cols()
    data_transformer.drop_and_reorder_cols()
    data_transformer.generate_summary()
    
    # save curated df as cvs
    SAVE_PATH = data_transformer.HOME / "data" / "curated" / f"{raw_data_file_name}_CURATED.csv"
    print(SAVE_PATH)
    data_transformer.raw_data.to_csv(str(SAVE_PATH))
    
if __name__ == "__main__":
    main()