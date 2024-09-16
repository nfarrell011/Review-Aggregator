## Data Transformers  
The transformer classes process and clean the raw extracted data and preparing the data for database insertion. Each raw data file required enough specific data transformation to warrant its transformer class. As a result there are $4$ data transformers classes, each is defined in a seperate ```.py``` file. The transformer classes are:
* ```OpenTableResDataTransformer```
* ```OpenTableReviewDataTransformer```
* ```YelpResDataTransformer```
* ```YelpReviewDataTransformer```
___

### Transformer Files:
There are transformer class files and transformer driver files. Each class is defined in a seperate file and each class has a seperate driver file. The driver files can be executed in any order. The classes perform tasks such as:
* Format adjustments: ```latin1``` to ```utf-8```
* String modifications: removing capital letters, white spaces, etc.
* Date format modifications
* Etc.

All the transformer classes take an input raw csv located in the ```/data/raw/``` folder and output a curated csv to the ```/data/curated/``` folder.

***Class Files***
```data_transformers/transformer_classes/open_table_res_data_transformer.py```
```data_transformers/transformer_classes/open_table_review_data_transformer.py```
```data_transformers/transformer_classes/yelp_res_data_transformer.py```
```data_transformers/transformer_classes/yelp_review_data_transformer.py```  

***Driver Files***
```data_transformers/transformer_classes/open_table_res_data_transformer_driver.py```
```data_transformers/transformer_classes/open_table_review_data_transformer_driver.py```
```data_transformers/transformer_classes/yelp_res_data_transformer_driver.py```
```data_transformers/transformer_classes/yelp_review_data_transformer_driver.py```

___
### Database Insertion  
Once the data has been curated it is ready to loaded into the database. This performed by the database manager described in the [Database README](/db_manager/README.md)