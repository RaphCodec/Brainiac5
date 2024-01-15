from pprint import pformat
from datetime import datetime
import json
import pandas as pd
import pyodbc
import ETLFlow
import tqdm

def main():
    # Get data. This data is from FiveThirtyEight and used for example purposes.
    df = pd.read_csv('https://raw.githubusercontent.com/fivethirtyeight/data/master/comic-characters/dc-wikia-data.csv')

    cursor,conn = ETLFlow.Connect(DEST_SQL_SERVER,DEST_SQL_DATABASE,DEST_SQL_DRIVER)
    
    #uncomment below to make table
    # query = ETLFlow.CreateTable()
    # cursor.execute(query)
    # conn.commit()
    # conn.close()
    
    #DIT stands for Data in Tanle
    DIT_query = f'SELECT name from [{DEST_SQL_TABLE}]'
    DIT = pd.read_sql(DIT_query, conn)
    
    df = df[~df['name'].isin(DIT['name'])] # filters for only new rows based on primary key.
    
    #insert new data
    if not df.empty:
        insert = ETLFlow.insert(list(df.columns),DEST_SQL_TABLE)
        
        #keep to ensure nulls are inserted properly into sql
        df  = df.astype(object).where(pd.notnull(df), None)
        
        cursor.fast_executemany = True
        cursor.executemany(insert, df.values.tolist())
        conn.commit()  


if __name__ == '__main__':
    start = datetime.now()
    print( f'\nScript starts:  { start  }' )


    config = "./Template.config.json"
    
    with open(config, 'r') as json_file:
        json_config = json.load(json_file)
    DEST_SQL_SERVER             = json_config['DEST_SQL_SERVER']
    DEST_SQL_DATABASE           = json_config['DEST_SQL_DATABASE']
    DEST_SQL_DRIVER             = json_config['DEST_SQL_DRIVER']
    DEST_SQL_TABLE              = json_config['DEST_SQL_TABLE']

    LOG_FILE_PATH               = json_config['LOG_FILE_PATH']

    try:

        print( f"Using the following config:\n\n{ pformat(json_config, sort_dicts=False) }" )
        print( f'{"*" * 40}\n' )

        main()

        end = datetime.now()
        print( f'\n{"*" * 40}' )
        print( f'Script ended:  {end}' )

    except Exception as e:
        print( f"ERROR: '{e}'\n" )

        end = datetime.now()
        print( f'\n{"*" * 40}' )
        print( f'Script ended:  {end}' )
       