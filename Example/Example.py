from loguru import logger
from icecream import ic
from datetime import datetime
import json
import pandas as pd
import pyodbc
import brainiac5 as b5

#fucntion to connect to servers
def Connect(Server:str, Database:str):
    conn_str = f"""
        Driver=SQL Server Native Client 11.0;
        Server={ Server };
        Database={ Database };
        Trusted_connection=YES;
    """
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    return cursor, conn

#setting up log file that is rewrtiten every time the scritp runs. 
logger.add('Example.log.txt', format="{time:YYYY-MM-DD HH:mm:ss} {level} | {message}", level="INFO", mode='w')

def ETL():
    '''
    This source is an csv from FiveThiryEight's github.
    Found at: https://github.com/fivethirtyeight/data/blob/master/avengers/avengers.csv
    '''
    df = pd.read_csv(SOURCE, encoding='latin-1')
    
    logger.info('Extract sucessful')
    
    '''
    These are some basic transformations and they assume that the source is very clean.
    However the below code can still be used with messy data in conjuction with other transformations
    and/or data cleaning.
    '''
    num_cols = [
        'Appearances',
        'Year',
        'Years since joining'
    ]
    
    df[num_cols] = df[num_cols].apply(pd.to_numeric)
    
    df['Current?'] = df['Current?'].replace({'YES':True, 'NO':False}).astype(bool)
    
    #creating a query object to create all queries related to df and DEST_TABLE
    query = b5.Query(df, DEST_TABLE)
    
    #Making Create Table Query. Comment out for Production
    ct = query.CreateTable(primary=['URL'], primaryName=f'PK__{DEST_TABLE}__ID')

    #this line helps ensure there are no errors inserting NULLs into SQL
    df = df.astype(object).where(pd.notnull(df), None) 

    conn = Connect(DEST_SERVER, DEST_DATABASE)[1]
    
    DIT_QRY = f'SELECT [URL] FROM {DEST_TABLE}'
    DIT = pd.read_sql(DIT_QRY, conn)
    
    df_insert = df[~df['URL'].isin(DIT['URL'])]
    df_update = df[df['URL'].isin(DIT['URL'])]
    
    #making insert and update queries.
    insert = query.Insert()
    update = query.Update(where=['URL'])
    
    #columns need to match order they show up in query
    df_update = df_update[list(df_update.columns[1:]) + [df_update.columns[0]]]
    
    if not df_insert.empty:
        b5.RunQuery(df_insert,insert,conn,BarDesc='Inerting Data')
    logger.info(f'Total rows inserted: {len(df_insert)}')
    
    if not df_update.empty:
        b5.RunQuery(df_update,update,conn,BarDesc='Updating Data')
    logger.info(f'Total rows updated: {len(df_update)}')
    
    conn.close()
    
if __name__ == '__main__':
    CONFIG = "Example.config.json"
    with open(CONFIG, 'r') as json_config:
        CONFIG_DICT = json.load(json_config)
        
    '''
    Loading CONFIG into variables to make script more readable.   
    Could use CONFIG_DICT['SOURCE'] throughout script if prefered.
    '''    
    SOURCE              = CONFIG_DICT['SOURCE']
    DEST_SERVER         = CONFIG_DICT['DEST_SERVER']
    DEST_DATABASE       = CONFIG_DICT['DEST_DATABASE']
    DEST_TABLE          = CONFIG_DICT['DEST_TABLE']
    
    logger.info(f'Using Config:\n{json.dumps(CONFIG_DICT, indent = 4)}\n')
    start = datetime.now()
    logger.info(f'Script Started')
    
    try:
        ETL()
        elapsed = datetime.now() - start
        logger.info(f"Script Ran Sucessfully. {elapsed.seconds // 3600} hours {elapsed.seconds % 3600 // 60} minutes {elapsed.seconds % 60} seconds elapsed")
    except Exception as e:
        elapsed = datetime.now() - start
        logger.error(f"Script Failed. {elapsed.seconds // 3600} hours {elapsed.seconds % 3600 // 60} minutes {elapsed.seconds % 60} seconds elapsed")
        # logger.exception(f'Error: {e}') 
        '''
        UNCOMMENT THE ABOVE LINE FOR A MORE DETAILED AND MORE FORMATTED ERROR MESSAGE
        '''
        from traceback import format_exc
        logger.error(f'Error: {e}')
        logger.error(f'\n{format_exc()}')