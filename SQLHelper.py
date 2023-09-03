import pandas as pd
import pyodbc

def Connect(Server,Database,Driver):
    conn = pyodbc.connect(
        f'''
        Driver={Driver};
        Server={Server};
        Database={Database};
        Trusted_Connection=yes;
        '''
        )
    cursor = conn.cursor()
    return cursor, conn    

def SSCreateTable(df,
                  table:str=None,
                  identity:bool=False,
                  numBuff:int=2, 
                  varBuff:int=5, 
                  keepFloats:list=[], 
                  chars:list=[],
                  pks:list=[], 
                  uniques:list=[], 
                  aliases:dict={},
                  saveSQL:bool = False
                  ):
     
    if table == None:
        raise Exception ('No table name provided. Cannot make statement')
    
    if identity == True and pks:
        raise Exception ('Cannot have both identity as primary key and pks not empty')
    
    create_table = f'CREATE TABLE [{table}] ('

    if identity == True:
        create_table += f'\n[ID] INT IDENTITY(1,1),\nCONSTRAINT [PK__{table}__ID]\nPRIMARY KEY CLUSTERED ([ID]),'

    for col in df.columns:
        if 'int' in str(df[col].dtype) and col in pks:
            create_table += f'\n[{col}] INT NOT NULL,'
        elif 'int' in str(df[col].dtype):
            create_table += f'\n[{col}] INT NULL,'
        elif 'float' in str(df[col].dtype) and col in keepFloats and col in pks:
            create_table += f'\n[{col}] FLOAT NOT NULL,'
        elif 'float' in str(df[col].dtype) and col in keepFloats:
            create_table += f'\n[{col}] FLOAT NULL,'
        elif 'float' in str(df[col].dtype):
            max_num = len(str(df[col].max()))
            create_table += f'\n[{col}] DECIMAL{max_num+numBuff,2} NULL,'
        elif 'date' in str(df[col].dtype) and col in pks:
            create_table += f'\n[{col}] DATE NOT NULL,'
        elif 'date' in str(df[col].dtype):
            create_table += f'\n[{col}] DATE NULL,'
        elif 'bool' in str(df[col].dtype) and col in pks:
            create_table += f'\n[{col}] BIT NOT NULL,'
        elif 'bool' in str(df[col].dtype):
            create_table += f'\n[{col}] BIT NULL,'
        elif col in chars and col in pks:
            max_num = df[col].map(len).max()
            create_table += f'\n[{col}] CHAR({max_num}) NOT NULL,'
        elif col in chars:
            max_num = df[col].map(len).max()
            create_table += f'\n[{col}] CHAR({max_num}) NULL,'
        elif col in pks:
            max_num = df[col].map(len).max()
            create_table += f'\n[{col}] VARCHAR({max_num+varBuff}) NOT NULL,'
        else:
            max_num = df[col].map(len).max()
            create_table += f'\n[{col}] VARCHAR({max_num+varBuff}) NULL,'
            
    if pks:
        pks = [f'[{pk}]' for pk in pks]
        create_table += f'\nCONSTRAINT [PK__{table}__ID] PRIMARY KEY CLUSTERED ({", ".join(map(str, pks))}),'
        
    if uniques:
        uniques = [f'[{uni}]' for uni in uniques]
        create_table += f'\nCONSTRAINT [UQ_{table}] UNIQUE ({", ".join(map(str, uniques))}),'

    create_table = create_table[:-1] + '\n)'

    
    if aliases:
        for key, value in aliases.items():
            create_table = create_table.replace(key,value)
    
    if saveSQL == True:
        with open('CreateTable.sql', 'w') as file:
            file.write(create_table)

    return create_table

def upsert(columns:list = [],
           table:str = None,
           insertOnly = False,
           upWhere:list=[]
           ):
    
    if table == None:
        raise Exception ('No table name provided.')

    insert_statement = f'INSERT INTO [{table}] (\n'
    
    insert_statement += f'[{columns[0]}]\n'
    
    for col in columns[1:]:
        insert_statement += f',[{col}]\n'
    insert_statement += f")\nVALUES(\n{('?,' * len(columns))[:-1]}\n)"

    if insertOnly == True:
        return insert_statement
    
    if not upWhere:
        raise Exception(f'Upwhere Empty. Cannot Update Tabble: [{table}].')
    
    up_list = list(set(columns) - set(upWhere))
    up_list.sort()

    update_statement = f'UPDATE [{table}]\nSET\n'
    update_statement += f'[{up_list[0]}]  = ?\n'
    for col in up_list[1:]:
            update_statement += f',[{col}]  = ?\n'

    if len(upWhere) == 1:
        update_statement += f'WHERE\n[{upWhere[0]}] = ?'
    else:
        update_statement += f'WHERE'
        for col in upWhere:
            update_statement += f'\n[{col}] = ?\nand'
        update_statement = update_statement[:-4]

    return insert_statement, update_statement