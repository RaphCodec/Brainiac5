from pprint import pformat
from datetime import datetime
import json
import pandas as pd
import pyodbc
import ETLFlow
import tqdm

def main():
    # Get data. This data is from FiveThirtyEight and used for example purposes.
    df = pd.read_csv('test.csv')

    cursor,conn = ETLFlow.Connect(DEST_SQL_SERVER,DEST_SQL_DATABASE,DEST_SQL_DRIVER)
    
    #uncomment below to make table
    # query = ETLFlow.CreateTable()
    # cursor.execute(query)
    # conn.commit()
    # conn.close()
    
    #DIT stands for Data in Tanle
    DIT_query = f'SELECT name from [{DEST_SQL_TABLE}]'
    DIT = pd.read_sql(DIT_query, conn)
    
    df1 = df[~df['name'].isin(DIT['name'])] # filters for only new rows based on primary key.
    df2 = df[df['name'].isin(DIT['name'])] # filters for only new rows based on primary key.
    df2 = df2 [[
        'page_id', 'urlslug', 'ID', 'ALIGN', 'EYE', 'HAIR', 'SEX',
       'GSM', 'ALIVE', 'APPEARANCES', 'FIRST APPEARANCE', 'YEAR', 'name'
    ]]
    
    #insert new data
    if not df1.empty:
        insert = ETLFlow.insert(list(df1.columns),DEST_SQL_TABLE)
        
        #keep to ensure nulls are inserted properly into sql
        df1  = df1.astype(object).where(pd.notnull(df1), None)
        
        cursor.fast_executemany = True
        cursor.executemany(insert, df1.values.tolist())
        conn.commit()  
    
    #update existing data
    if not df2.empty:
        try:
            update_query =  ETLFlow.update(list(df2.columns),DEST_SQL_TABLE,'name')
            df2  = df2.astype(object).where(pd.notnull(df2), None)
            
            cursor.fast_executemany = True
            cursor.executemany(update_query, df2.values.tolist())
            conn.commit() 
        except Exception as e:
            # Clear existing data in the CSV file
            with open('update_errors.csv', 'w') as error_file:
                error_file.write('Index,' + ','.join(df2.columns) + '\n')
            
            for index, row in df2.iterrows():
                try:
                    cursor.execute(update_query, row.tolist())
                    conn.commit()
                except pyodbc.Error as e:
                    # Log the error row to the CSV file with its index
                    row.to_csv('row.csv')
                    
            # print(error_rows)
            # print(len(error_rows))

        
    conn.close()
        


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
       