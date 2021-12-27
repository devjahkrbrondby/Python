from numpy import record
import pypyodbc as odbc  
import pandas as pd
import csv  

"""
Step 3.1 Create SQL Servre Connection String
"""

conn = odbc.connect('Driver={SQL Server};'
                      'LAPTOP-3EFU92FT\MSSQLSERVER01;'
                      'Database=tSQLt_Database;'
                      'Trusted_Connection=yes;')

# my sql account/user
# conn = odbc.connect('Driver={SQL Server};'
#                       'LAPTOP-3EFU92FT\MSSQLSERVER01;'
#                       'Database=tSQLt_Database;'
#                       'User ID=Test_UFST_Account;Password=TestAccount;')


sql_query = pd.read_sql_query(''' 
                               SELECT * FROM Lag1_Table1
                              '''
                              ,conn)

df = pd.DataFrame(sql_query)
df.to_csv (r'C:\\Users\\JacobHøj-Kristensen\\source\\repos\\Python\\records.csv', index = False)
print(df)
