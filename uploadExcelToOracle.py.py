import pandas as pd
import numpy as np
import cx_Oracle
from sqlalchemy import types, create_engine
from types import *
from pprint import pprint
import cx_Oracle
import sqlalchemy as sa
from datetime import datetime
import time


#All static vars go here
dbType = "OracleDB"

dbUser = "achem_drm"
dbPassword = "p1rosalma"
dbHost = "dalbcedd1.na.xom.com"
dbName = "bced"
dbPort = "55009"

#OracleDB Connection strings
oracleConnStr = 'oracle+cx_oracle://'+ dbUser + ':' + dbPassword + "@" + dbHost +':'+ dbPort +'/' + dbName
oracleDatabaseEngine = sa.create_engine(oracleConnStr)
oracleDbConnection = oracleDatabaseEngine.connect()


#Declare location of Config file and root directory for data here
#Assumes that these directories will be accessible to the code
configFile = "workspace/de/drm-de/sampleData/config.xlsx"
configSheet = "config"
dataDirectory = "workspace/de/drm-de/sampleData/"



#Reading the config file here
configDf = pd.read_excel(open(configFile, 'rb'), sheet_name=configSheet, encoding='latin-1')
configDf = configDf.dropna(how='all')

###################################################################################################
#     Configuration file format
# 
#     fileName -> Name of file to be uploaded
#     sheetName -> Name of sheet in the excel file being uploaded
#     headerRow -> Row number to be treated as header
#     skipFromRow -> Rows to be skipped which do not contain data (Start index)
#     skipTillRow -> Row to be skipped whichh do not contain data (End index)
#     columns -> List like column names (A:B, C, F;K, z)
#     columnsRenamed -> Comma seperated key:value pair for renaming columns (A:B, C:D)
#     databaseName -> Oracle db name where data will be uploaded
#     tableName -> Oracle table name where data will be uploaded
#####################################################################################################


def runOracleQuery(sqlQuery):
    ##################################################################################
    # Helper function
    # Function to run Oracle Query and print output
    # Takes input SQL Query and database congigurations
    # DB configuration is expected to be pre-declared in scope to the function call
    ##################################################################################
    try:
        oracleTns = cx_Oracle.makedsn(dbHost, dbPort, dbName)
        dbConnection = cx_Oracle.connect(dbUser, dbPassword, oracleTns)
        
        cursor = dbConnection.cursor()
        cursor.execute(sqlQuery)
        dbConnection.commit()

        try:
            data = cursor.fetchall()
            pprint(cursor.description)
            pprint(data)
        except:
            print("DEBUG: Not a data query, no data to print")

        cursor.close()
        dbConnection.close()
        print("DEBUG: Query executed")
        
    except Exception as e:
        print("ERROR: Something went wrong executing the query")
        print("ERROR: " + str(type(e).__name__))
        print("ERROR: " + str(e))   


def getTableAsDf(dbConnection, tableName):
    ##################################################################################
    # Helper function
    # Function to run Oracle Query and print output
    # Takes input SQL Query and database congigurations
    # No need to pass DB configuration if database variables are already declared where the function is called
    ##################################################################################
    query = "SELECT * FROM " + tableName
    try:
        readDf = pd.read_sql(query, con=dbConnection)
        return readDf
    except Exception as e:
        print("ERROR: Something went wrong while fetching the table")
        print("ERROR: " + str(type(e).__name__))
        print("ERROR: " + str(e))
        return pd.DataFrame()



def truncateTable(tableName, taskId):
    ##################################################################################
    # Helper function
    # Function to truncate a table from OracleDB
    # Takes input table name
    ##################################################################################
    try:
        sqlQuery = "TRUNCATE TABLE " + tableName
        runOracleQuery(sqlQuery)
        print("DEBUG: Truncated table " + tableName)
        return "Success"
    except Exception as e:
        print("ERROR: Something went wrong while truncating the table " + tableName + " in task id " + taskId)
        print("ERROR: " + str(type(e).__name__))
        print("ERROR: " + str(e))
        return "Failure"


def renameColums(df, columnsRenamed, taskId):
    ##################################################################################
    # Function to rename column names for a dataframe
    # Expects new names for EVERY column in the dataframe

    # columnsRenamed -> Comma seperated string containing new names for columns
    # taskId -> Task ID is passed to the function for debugging purpose
    # df -> Pandas dataframe to which the column rename function is applied 
    ##################################################################################
    
    if columnsRenamed.lower() in ["", None, "nan"]:
        return df
    
    columnsRenamed = columnsRenamed.split(',')   
    if len(df.columns.values) != len(columnsRenamed):
        print("ERROR: The number of columns to be renamed is not correct")
        return pd.DataFrame()
    
    columnsToRename = {}
    
    try:
        for i in range(len(columnsRenamed)):
            columnsToRename[df.columns.values[i].strip()] = columnsRenamed[i].strip()
        df = df.rename(columns=columnsToRename)
        return df
    
    except Exception as e:
        print("ERROR: Something went wrong while renaming columns for task id " + taskId)
        print("ERROR: " + str(type(e).__name__))
        print("ERROR: " + str(e))
        return pd.DataFrame()



def changeDataType(df, changeDataTypeConditions, taskId):
    ##################################################################################
    # Function to change data types for columns in a dataframe

    # changeDataTypeConditions -> Comma seperated key value pairs of format COLUMN-NAME: DATA-TYPE
    # taskId -> Task ID is passed to the function for debugging purpose
    # df -> Pandas dataframe to which the column data type conversion is applied
    
    # Supported Data types are:
    # object, int64, float64, datetime64, bool
    ##################################################################################
    
    if changeDataTypeConditions.lower() in ["", None, "nan"]:
        return df 
    
    changeDataTypeConditions = changeDataTypeConditions.split(',')
    changeDataTypeDictionary = {}
    
    try:
        for i in changeDataTypeConditions:
            i = i.strip()
            i = i.split(':')
            changeDataTypeDictionary[i[0].strip()] = i[1].strip()
            df = df.astype(changeDataTypeDictionary)
        return df
    
    except Exception as e:
        print("ERROR: Something went wrong while changing data type for task id " + taskId)
        print("ERROR: " + str(type(e).__name__))
        print("ERROR: " + str(e))
        return pd.DataFrame()



def filterData(df, filterDataConditions, taskId):
    ##################################################################################
    # Function to filter rows from data frame

    # filterDataConditions -> Expression for the filter to be applied
    # taskId -> Task ID is passed to the function for debugging purpose
    # df -> Pandas dataframe to which the filter is applied
    ##################################################################################
    if filterDataConditions.lower() in ["", None, "nan"]:
        return df 
    
    filterDataConditions = filterDataConditions.replace(" ","")
    
    if "AND" in filterDataConditions:
        filterDataConditions = filterDataConditions.replace("AND", "&")
    if "OR" in filterDataConditions:
        filterDataConditions = filterDataConditions.replace("OR", "|")
    
    try:
        df = df.query(filterDataConditions)
        return df
    except Exception as e:
        print("ERROR: Something went wrong while applying filter conditions for task id " + taskId)
        print("ERROR: " + str(type(e).__name__))
        print("ERROR: " + str(e))
        return pd.DataFrame()



def insertAdditionalColumns(df, insertColumns, taskId):
    ##################################################################################
    # Function to insert specific new columns into a dataframe

    # insertColumns -> Comma seperated list of new columns to be inserted
    # taskId -> Task ID is passed to the function for debugging purpose
    # df -> Pandas dataframe into which the new columns are inserted
    
    # Supported columns:
    # UPLOAD_TIME
    ##################################################################################
    
    if insertColumns.lower() in ["", None, "nan"]:
        return df 

    insertColumns = insertColumns.split(',')
    
    for i in insertColumns:
        if i.strip().lower() == "upload_time":
            now = datetime.now()
            current_time = now.strftime("%H:%M:%S")
            df['UPLOAD_TIME'] = (current_time)
            
        else:
            continue
            
    return df


def replaceData(df, replaceDataConditions, taskId):
    ##################################################################################
    # Function to replace values in a dataframe

    # replaceDataConditions -> Key Value pair where key is the column name, and value is a doctionary with data replacement conditions
    # taskId -> Task ID is passed to the function for debugging purpose
    # df -> Pandas dataframe to which the data replacements are applied
    ##################################################################################
    
    if replaceDataConditions.lower() in ["", None, "nan"]:
        return df 
    
    replaceDataConditions = replaceDataConditions.split(',')
    
    try:
        for i in replaceDataConditions:
            i = i.strip()
            i = i.split(':',1)
            col_name=i[0]
            conditions=i[1].replace('{','').replace('}',"").split('|')
            col_type=str(df[col_name].dtype)
            for condition in conditions:
                condition = condition.strip()
                old,new = condition.split(":")
                if old.lower() in ['nan','n.a.','n.a','na','']:
                    if 'float' in col_type:
                        df[col_name]=df[col_name].fillna(float(new))
                    elif 'int' in col_type:
                        df[col_name]=df[col_name].fillna(int(new))
                    elif 'obj' in col_type:
                        df[col_name]=df[col_name].replace(old,new)
                else:
                    if 'float' in col_type:
                        df[col_name]=df[col_name].replace(float(old),float(new))
                    elif 'int' in col_type:
                        df[col_name]=df[col_name].replace(int(old),int(new))
                    elif 'obj' in col_type:
                        df[col_name]=df[col_name].replace(old,new)
        return df
    except Exception as e:
        print("ERROR: Something went wrong while replacing data for task id " + taskId)
        print("ERROR: " + str(type(e).__name__))
        print("ERROR: " + str(e))
        return pd.DataFrame()



def formatDateTime(df, formatDateTimeConditions, taskId):
    ##################################################################################
    # Function to format dates in a dataframe

    # formatDateTimeConditions -> Comma seperated key value pair where key is column name and value is new date format
    # taskId -> Task ID is passed to the function for debugging purpose
    # df -> Pandas dataframe to which the date transformations are applied
    ##################################################################################
    
    if formatDateTimeConditions.lower() in ["", None, "nan"]:
        return df 
    
    try:
        formatDateTimeConditions=formatDateTimeConditions.split(',')
        for i in formatDateTimeConditions:
            i = i.strip()
            col,new_format = i.split(':',1)
            df[col]=pd.to_datetime(df[col]).dt.strftime(new_format)
        return df
    except Exception as e:
        print("ERROR: Something went wrong while formating datetime for task id " + taskId)
        print("ERROR: " + str(type(e).__name__))
        print("ERROR: " + str(e))
        return pd.DataFrame()



def shuffleDf(df, columnNames, additionalColumns, taskId):
    ##################################################################################
    # Function to re-shuffle a dataframe columns

    # columnNames -> Comma seperated list of column names
    # additionalColumns -> Comma seperated list of column added programatically
    # df -> Pandas dataframe to which the  column shuffle is applied
    ##################################################################################
    
    try:
        shuffledOrder = []
        columnNames = columnNames.split(',')
        for i in columnNames:
            shuffledOrder.append(i.strip())
            
        additionalColumns = additionalColumns.split(',')
        for i in additionalColumns:
            shuffledOrder.append(i.strip())
            
        df = df[shuffledOrder]
        return df
    except Exception as e:
        print("ERROR: Something went wrong while shuffling columns for task id " + taskId)
        print("ERROR: " + str(type(e).__name__))
        print("ERROR: " + str(e))
        return pd.DataFrame()




def dfInsertToTable(df, tableName, taskId, startRow = 0):
    startRow = int(startRow)

    oracleTns = cx_Oracle.makedsn(dbHost, dbPort, dbName)
    dbConnection = cx_Oracle.connect(dbUser, dbPassword, oracleTns, encoding = "UTF-8", nencoding = "UTF-8")
    
    cursor = dbConnection.cursor()
    cursor1 = dbConnection.cursor()
    cursor1.execute("SELECT * FROM " + tableName )
    dbConnection.commit()
    rowsTemp = cursor1.fetchall()
    cursor.bindarraysize = len(rowsTemp)
    db_types = (d[1] for d in cursor1.description)
    cursor.setinputsizes(*db_types)
    cursor1.close()
            
    trackerIndex = 0
    for index, row in df[startRow:].iterrows():
        try:
            trackerIndex = index

            rowData = ""
            counter = 1
            for i in row:
                rowData = rowData + " :" + str(counter) + " ,"
                counter += 1
            rowData = rowData[:-2]

            columnNamesString = []
            for i in df.columns.values:
                temp = '"' + i + '"'
                columnNamesString.append(temp)
            columns = (',').join(columnNamesString)

            sqlQuery = "INSERT INTO " + tableName + " (" + columns +  ") VALUES" + "(" + rowData + ")"
            
            map(lambda x: x.encode('utf-8'), row)
            
            cursor.execute(sqlQuery, row)
            dbConnection.commit()

            if index % 100 == 0:
                print("DEBUG: Writing row " + str(index) + " for task id " + str(taskId))
        
        except Exception as e:
            #Failed at row startRow
            print(row)
            print("ERROR: Something went wrong while writing to table " + tableName + " for task id " + taskId)
            print("DEBUG: Failed at row " + str(trackerIndex))
            print("ERROR: " + str(type(e).__name__))
            print("ERROR: " + str(e))
            cursor.close()
            dbConnection.close()
            return "Failure"
        
    print("DEBUG: Insert successful")
    cursor.close()
    dbConnection.close()
    return "Success"




def dfUpdateToTable(df, tableName, taskId, upsertKey, startRow = "0"):

    startRow = int(startRow.strip())
    
    upsertKeys = upsertKey.strip().split(',')
    
    oracleTns = cx_Oracle.makedsn(dbHost, dbPort, dbName)
    dbConnection = cx_Oracle.connect(dbUser, dbPassword, oracleTns, encoding = "UTF-8", nencoding = "UTF-8")
    
    cursor1 = dbConnection.cursor()
    cursor = dbConnection.cursor()

    cursor1.execute("SELECT * FROM " + tableName )
    dbConnection.commit()
    rowsTemp = cursor1.fetchall()
    cursor.bindarraysize = len(rowsTemp)
    db_types = (d[1] for d in cursor1.description)
    cursor.setinputsizes(*db_types)
    cursor1.close()
            
    trackerIndex = 0
    for index, row in df[startRow:].iterrows():
        try:
            trackerIndex = index

            setValues = ""
            for i in range(len(df.columns.values)):
                setValues = setValues + "\""+ df.columns.values[i] + "\" = :" + str(i) + ", "
                counter = i
            
            counter += 1
            setValues = setValues[:-2]

            whereClause = ""
            queryParams = row.values.tolist()
            
            for i in upsertKeys:
                whereClause = whereClause + "\"" + i.strip() + "\" = :" + str(counter) + " AND "
                queryParams.append(row[i.strip()])
                counter += 1
                
            whereClause = whereClause[:-4]
                
            sqlQuery = "UPDATE " + str(tableName) + " SET " + str(setValues) + " WHERE " + whereClause
            
            map(lambda x: x.encode('utf-8'), queryParams)
            
            cursor.execute(sqlQuery, queryParams)
            dbConnection.commit()
            
            if index % 100 == 0:
                print("DEBUG: Writing row " + str(index) + " for task id " + str(taskId))
            
        except Exception as e:
            #Failed at row startRow
            print("ERROR: Something went wrong while writing to table " + tableName + " for task id " + taskId)
            print("DEBUG: Failed at row " + str(trackerIndex))
            print("ERROR: " + str(type(e).__name__))
            print("ERROR: " + str(e))
            cursor.close()
            dbConnection.close()
            return "Failure"
        
    cursor.close()  
    dbConnection.close() 
    print("DEBUG: Update successful")
    return "Success"



def writeDfToDb(df, tableName, upsertKey, truncateBool, resumeFromRow, taskId):
    
    if dbType == "OracleDB":

        if resumeFromRow not in ["", "0", 0, "nan", None]:
            #If resuming, then no need to truncate
            
            if upsertKey in ["", "nan",None]:
                #Not upserting, simply insert data
                dfInsertToTable(df, tableName, taskId, resumeFromRow)
            else:
                #Upserting / Updating the data for a particular key
                dfUpdateToTable(df, tableName, taskId, upsertKey, resumeFromRow)
                
        else:
            if truncateBool in ["1", 1, True, "True"]:
                deleteStatus = truncateTable(tableName, taskId)
                if deleteStatus == "Failure":
                    return "Failure"
            
            if upsertKey in ["", "nan",None]:
                #Not upserting, simply insert data
                dfInsertToTable(df, tableName, taskId)
                
            else:
                #Upserting / Updating the data for a particular key
                dfUpdateToTable(df, tableName, taskId,  upsertKey)



for index, row in configDf.iterrows():
    #Iterate the config for individual data transformations
    
    #Record the start time for debuging purpose
    start_time = time.time()

    ####################################################################################################
    #Reading configurations data here
    
    #Adding '1' to ignore column will skip the test
    if row['ignore'] in ['1', 1]:
        continue
    
    taskId = row['id']
    if taskId == "":
        continue
    
        
    dataFile = dataDirectory + str(row['fileName']).strip()
    sheetName = str(row['sheetName']).strip()
    
    skipFromRow = row['skipFromRow']
    skipTillRow = row['skipTillRow']
    headerRow = row['headerRow']
    
    columns = str(row['columns']).strip()
    
    columnsRenamed = str(row['columnsRenamed']).strip()
    filterDataConditions = str(row["filterDataCondition"]).strip()
    replaceDataConditions = str(row["replaceDataCondition"]).strip()
    changeDataTypeConditions = str(row["changeDataTypeCondition"]).strip()
    formatDateTimeConditions = str(row["formatDateTimeCondition"]).strip()

    tableName = str(row['tableName']).strip()
    
    truncateBool = str(row['truncateBool']).strip()
    upsertKey = str(row['upsertKey']).strip()
        
    #Insert Additional columns
    additionalCols = str(row['additionalCols']).strip()
    ####################################################################################################
     

        
        
        
    ####################################################################################################
    #Making sanity checks on basic data
    
    print()
    print("DEBUG: Running task " + str(taskId) + " for table " + str(tableName))
    
    if type(taskId)!=int:
        print("ERROR: 'Task id' should be integer. Incorrect format for task id " + taskId)
        continue
    else:
        taskId = str(taskId)
       
    try:
        headerRow = int(row['headerRow'])
    except Exception as e:
        print("WARN: Header row empty or incorrect format. Considering 1st row as header")
        headerRow = 1
        
    try:
        skipFromRow = int(row['skipFromRow']) - 1
    except Exception as e:
        print("WARN: 'Skip From Row' empty or incorrect format. Not applying row skips")
        skipFromRow = 0
        
    try:
        skipTillRow = int(row['skipTillRow']) - 1
    except Exception as e:
        print("WARN: 'Skip Till Row' empty or incorrect format. Not applying row skips")
        skipTillRow = 0
        
    if truncateBool not in ["", "0", "1", "nan"]:
        print("ERROR: Invalid truncate Bool, please check the configuration for task id " + taskId)
        continue
                
    if columns in ["", None, "nan"]:
        columns = None
    
    if str(row['resumeFromRow']) in ["", "nan", 0, None, "0", "Nan", "NaN"]:
        resumeFromRow = 0
    else:
        #Restart processing from row (Failure handling)
        resumeFromRow = int(row['resumeFromRow'])
        
    ####################################################################################################
    
    
    
    #Read the excel file to be uploaded
    dataDf = pd.read_excel(open(dataFile, 'rb'), sheet_name=sheetName ,  usecols=columns, skiprows=range(skipFromRow, skipTillRow))

    
    ####################################################################################################
    #Applying data transformations
    
    #Rename columns using input list of format: old1:new1, old2:new2......
    dataDf = renameColums(dataDf, columnsRenamed, taskId)
    if (dataDf.empty == True):
        continue
        
    
    #Apply filters on excel data using input conditions of format: col1<10 & col2>100 | col3=100 | col4!=19
    dataDf = filterData(dataDf, filterDataConditions, taskId)
    if (dataDf.empty == True):
        continue

    #Replace data for selected columns based on input conditions of format: col5:{25:26}, col6:{123:124}, col7:{nan:0}    
    dataDf = replaceData(dataDf, replaceDataConditions, taskId)
    if (dataDf.empty == True):
        continue    
    
    #Format DateTime columns based on input conditions of format: col8:%Y-%b-%d, col9:%Y-%b-%d %H:%M:%S %p
    dataDf = formatDateTime(dataDf, formatDateTimeConditions, taskId)
    if (dataDf.empty == True):
        continue 
        
        
    #Insert additional columns 
    dataDf = insertAdditionalColumns(dataDf, additionalCols, taskId)
    if (dataDf.empty == True):
        continue 
        
    #Change data type for selected columns based on input conditions of format: col5:float, col6:int, col7:str
    dataDf = changeDataType(dataDf, changeDataTypeConditions, taskId)
    if (dataDf.empty == True):
        continue 
        
    dataDf = shuffleDf(dataDf, columnsRenamed, additionalCols, taskId)
    if (dataDf.empty == True):
        continue 
    ####################################################################################################
    
    try:
        writeDfToDb(dataDf, tableName, upsertKey, truncateBool, resumeFromRow, taskId)
        print("DEBUG: Time for execution --- %s seconds ---" % (time.time() - start_time))
        
    except Exception as e:
        print("ERROR: Something went wrong in non database operations while writing for task id " + taskId)
        print("ERROR: " + str(type(e).__name__))
        print("ERROR: " + str(e))

