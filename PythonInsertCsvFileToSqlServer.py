import pypyodbc as odbc  
import pandas as pd

# Import data from csv file

df = pd.read_csv('C:\\Users\\JacobHøj-Kristensen\\Desktop\\tSQLt_TestProject_Tutorial\\ExportCSVTable1.csv')
df

# clean up data
# nedenstående kolonne eksiterer ikke i mit datasæt, så det ar bare for eksemplets skyld
# pd.to_datetime(df['Published Date']).iloc[:5]
# test

# username = 'Test_UFST_Account' 
# password = 'TestAccount' 


## df.drop(df.query('Location.isnull() | Status.isnull()').index, inplace=True)


"""
Step 2.2 Specify columns we want to import
"""
columns = ['EmployeeId', 'Education', 'Subjects', 'LenghtOfEducation']

df_data = df[columns]
records = df_data.values.tolist()


"""
Step 3.1 Create SQL Servre Connection String
"""

DRIVER = 'SQL Server'
SERVER_NAME = 'LAPTOP-3EFU92FT\MSSQLSERVER01' 
DATABASE_NAME = 'tSQLt_Database' 

def connection_string(driver, server_name, database_name):
    conn_string = f"""
        DRIVER={{{driver}}};
        SERVER={server_name};
        DATABASE={database_name};
        Trust_Connection=yes;        
    """
    return conn_string

"""
Step 3.2 Create database connection instance
"""
try:
    conn = odbc.connect(connection_string(DRIVER, SERVER_NAME, DATABASE_NAME))
except odbc.DatabaseError as e:
    print('Database Error:')    
    print(str(e.value[1]))
except odbc.Error as e:
    print('Connection Error:')
    print(str(e.value[1]))


"""
Step 3.3 Create a cursor connection and insert records
"""

sql_insert = '''
    INSERT INTO Lag1_Table1 
    VALUES (?, ?, ?, ?)
'''

try:
    cursor = conn.cursor()
    cursor.executemany(sql_insert, records)
    cursor.commit();    
except Exception as e:
    cursor.rollback()
    print(str(e[1]))
finally:
    print('Task is complete.')
    cursor.close()
    conn.close()


