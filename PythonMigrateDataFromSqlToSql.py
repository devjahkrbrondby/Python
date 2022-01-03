from numpy import record
import pypyodbc as odbc  
import pandas as pd
import csv  
# import urllib
# import sqlalchemy as sa


"""
Step 3.1 Create SQL Servre Connection String
"""

# conn = odbc.connect('Driver={SQL Server};'
#                       'SERVER=LAPTOP-3EFU92FT\MSSQLSERVER01;'
#                       'Database=tSQLt_Database;'
#                       'Trusted_Connection=yes;')
conn = odbc.connect('Driver={SQL Server};'
                      'SERVER=.;'
                      'Database=tSQLt_Database;'
                      'Trusted_Connection=yes;')

# my sql account/user
# conn = odbc.connect('Driver={SQL Server};'
#                       'LAPTOP-3EFU92FT\MSSQLSERVER01;'
#                       'Database=tSQLt_Database;'
#                       'User ID=Test_UFST_Account;Password=TestAccount;')


sql_query = pd.read_sql_query(''' 
                               SELECT [EmployeeId],[Education],[Subjects],[LenghtOfEducation] FROM Lag1_Table1
                              '''
                              ,conn)

df = pd.DataFrame(sql_query)
print(df)
# Insert Dataframe into SQL Server:
cursor = conn.cursor()
cursor.fast_executemany = True
for row_count in range(0, df.shape[0]):
   chunk = df.iloc[row_count:row_count + 1,:].values.tolist()
   tuple_of_tuples = tuple(tuple(x) for x in chunk)
   cursor.executemany("insert into dbo.Lag1_Table4" + " (EmployeeId,Education,Subjects,LenghtOfEducation) values (?,?,?,?)",tuple_of_tuples)
   conn.commit()
cursor.close()



      


