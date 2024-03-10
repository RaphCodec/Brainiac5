import logging
from datetime import datetime
import json
import pandas as pd
import pyodbc
import brainiac5 as b5

#setting up log file
logging.basicConfig(filename='Template.log.txt', 
                    level=logging.INFO,
                    filemode='w',
                    format='%(asctime)s %(levelname)s : %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p'
                    )

def Extract():
    data = {
    'ID': [1, 2, 3, 4, 5],
    'float_column': [1.1, 2.2, 3.3, 4.4, 5.5],
    'string_column': ['apple', 'banana', 'cherry', 'date', 'elderberry'],
    'boolean_column': [True, False, True, False, True],
    'datetime_column': ['2023-01-01 12:58:58', '2023-01-02 12:58:58', '2023-01-03 12:58:58', '2023-01-04 12:58:58', '2023-01-05 12:58:58'],
    }
    
    df = pd.DataFrame(data)
    
    
    logging.info('Extract sucessful')
    Transform(df)
    
def Transform(df):
    
    #some basic data validation/transformation to get started
    num_cols = [
        'ID',
        'float_column'
    ]
    
    df[num_cols] = df[num_cols].apply(pd.to_numeric)
    
    date_cols = [
        'datetime_column'
    ]
    
    df[date_cols] = df[date_cols].apply(pd.to_datetime)
    
    Load(df)
    
def Load(df):
    print(df)
    

    
    

if __name__ == '__main__':
    CONFIG = "Template.config.json"
    with open(CONFIG, 'r') as json_config:
        CONFIG_DICT = json.load(json_config)
    
    print(json.dumps(CONFIG_DICT, indent = 4))
    logging.info(f'Using Config:\n{json.dumps(CONFIG_DICT, indent = 4)}\n')
    start = datetime.now()
    logging.info(f'Script Started')
    Extract()
    
    elapsed = datetime.now() - start