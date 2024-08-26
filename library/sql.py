
import json
import numpy as np
import pandas as pd
from sklearn import preprocessing
import itertools
import statsmodels.api as sm
from sqlalchemy import create_engine, Table, MetaData, insert, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select, func
from sqlalchemy import and_
from dotenv import load_dotenv
from core import exceptionOutput

load_dotenv(dotenv_path='../.env')
import os

def connectToSQL(
        sql_username: str = 'root', 
        sql_database: str = 'mid_db', 
        sql_port: int = 3306, 
        db: bool = False, **kwargs
    ):

    sql_password = os.getenv('SQL_PASSWORD')


    engine = create_engine(f'mysql+pymysql://{sql_username}:{sql_password}@{sql_username}:{sql_port}/{sql_database}')
    connection = engine.connect()

    metadata = MetaData()
    metadata.reflect(bind=engine)

    return engine, connection, metadata

def sqlORM(selectColumns: list = None, tableName: str = None, dateCol:str = None, dateStart: str = None, dateEnd: str = None, userList: list = None, **kwargs) -> pd.DataFrame:
    """ SQL ORM
    
        - This will return data from a sql table that specifies the conditions outlined in the arguments
        : selectColumns --> List of columns to return
        : tableName --> What table to look in
        : dateCol --> What column we are looking at
        : dateStart/dateEnd --> Start end dates inclusive
        : userList --> List of users on which to apply these filters

        E.g.,

        results = sqlORM(
            selectColumns = ['userId','attitude','energy'],
            tableName = '01_processedCheckIns',
            dateCol = 'startTime',
            dateStart = '2024-07-16',
            userList = [os.getenv("SEAN"),os.getenv("NISHIT")]
        )
    
    """

    engine, connection, metadata = connectToSQL()
    table = metadata.tables[tableName]
    
    Session = sessionmaker(bind=engine)
    session = Session()

    if selectColumns:
        query = session.query(*[getattr(table.c, col) for col in selectColumns])
    else:
        query = session.query(table)

    if dateStart:
        query = query.filter(getattr(table.c, dateCol) >= dateStart)

    if dateEnd:
        query = query.filter(getattr(table.c, dateCol) <= dateEnd)

    # Apply user ID filter if provided
    if userList:
        query = query.filter(table.c.userId.in_(userList))

    results = query.all()
    
    session.close()

    resultsDf = pd.DataFrame(results)

    return resultsDf

def isSQLCompatible(df: pd.DataFrame, column_name: str = '', **kwargs):
    import datetime
    if isinstance(df[column_name].iloc[0], datetime.date) | isinstance(df[column_name].iloc[0], datetime.datetime):
        return True
    
    # Get the data type of the column
    dtype = df[column_name].dtype
    
    # Define SQL compatible Pandas data types
    sqlTypes = ['int64', 'float64', 'object', 'bool', 'datetime64[ns]', 'timedelta[ns]']
    timeTypes = ['datetime','timedelta','datetime64','date','datetime.date']
    
    # Check if the column's data type is in the list of SQL compatible types
    if (str(dtype) in sqlTypes) | (str(dtype).split('[')[0] in timeTypes):
        # For object types, ensure all elements are basic data types (int, float, str, bool, None)        
        if dtype == 'object':
            for element in df[column_name]:
                if not isinstance(element, (int, float, str, bool, type(None))):
                    return False
        
        return True
    else:
        return False

def saveDfToSQL(
        df: pd.DataFrame, 
        targetTable: str = '', 
        colsToSave: list = [], 
        index: bool = False, 
        colKey: str = '', 
        primaryKeys: list = [], 
        SQLColType: str = '',
        update: bool = False,
    **kwargs):
    import json

    engine, connection, metadata = connectToSQL()

    tempDfSQL = df.copy(deep=True)

    tempDfSQL = tempDfSQL.where(pd.notnull(tempDfSQL), None)

    for col in tempDfSQL.columns:
        if not isSQLCompatible(tempDfSQL, col):
            try:
                tempDfSQL[col] = tempDfSQL[col].apply(lambda v: json.dumps(v))
            except:
                tempDfSQL[col] = tempDfSQL[col].astype(str)


    # Don't worry about the upsert if we aren't providing primary keys
    if primaryKeys == []:
        tempDfSQL.to_sql(name = targetTable, con = engine, if_exists='append', index = index)
        connection.close()
        engine.dispose()
    # Handle upsert in cases of primary keys
    else:
        try:
            query = f"""
            SELECT {', '.join(primaryKeys)}
            FROM {targetTable}
            """
            existing_records = pd.read_sql_query(query, engine)

            existingRecordsSet = list(set(tuple(x) for x in existing_records.to_numpy()))

            def checkIfExists(row: pd.Series = None, primaryKeys: list = [], existingRecordsSet: list = [], **kwargs):
                rowVals = tuple(row[k] for k in primaryKeys)
                
                if rowVals in existingRecordsSet:
                    return True
                else:
                    return False

            tempDfSQL['exists'] = tempDfSQL.apply(lambda row: checkIfExists(row = row, primaryKeys = primaryKeys, existingRecordsSet = existingRecordsSet), axis=1)


            # Split df into existing and new records
            df_existing = tempDfSQL[tempDfSQL['exists'] == True]
            df_new      = tempDfSQL[tempDfSQL['exists'] == False]

            
            # Clean up
            df_existing.drop(columns=['exists'], inplace=True) 
            tempDfSQL.drop(columns=['exists'], inplace=True) 
            df_new.drop(columns=['exists'], inplace=True) 
            
            # Construct statements
            uniqueIDStatement = ""
            for i, primaryKey in enumerate(primaryKeys):
                if i == 0:
                    uniqueIDStatement += f"{primaryKey} = :{primaryKey}"
                else:
                    uniqueIDStatement += f" AND {primaryKey} = :{primaryKey}"

            columnStatement = ""
            for i, col in enumerate(tempDfSQL.columns):
                if i == 0:
                    columnStatement += f"{col} = :{col}"
                else:
                    columnStatement += f", {col} = :{col}"
            
            # Step 2: Update existing records
            if update:
                for _, row in df_existing.iterrows():
                    update_query = text(f"""
                        UPDATE {targetTable}
                        SET {columnStatement}
                        WHERE {uniqueIDStatement}
                    """)

                    result = connection.execute(update_query, row.to_dict())
                    connection.commit()

            if len(df_new) > 0:
                df_new.to_sql(targetTable, con=engine, if_exists='append', index=index)
                connection.close()
                engine.dispose()
        except Exception as e:
            exceptionOutput(e)
            raise e
        