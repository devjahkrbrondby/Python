import pypyodbc as odbc  
import pandas as pd
import csv  

# Import data from csv file
data = pd.read_csv('C:\\Users\\JacobHøj-Kristensen\\Desktop\\tSQLt_Bbr_Test_Project\\data.csv')
df = pd.DataFrame(data)
print(df)

# clean up data
# nedenstående kolonne eksiterer ikke i mit datasæt, så det ar bare for eksemplets skyld
# pd.to_datetime(df['Published Date']).iloc[:5]


## Step 3.1 Create SQL Servre Connection String
# localhost\MSSQLSERVER01

# Connect to SQL Server
# conn = odbc.connect('Driver={SQL Server};'
#                       'Server=localhost\MSSQLSERVER01;'
#                       'Database=ProdBbrUFST;'
#                       'Trusted_Connection=yes;')
# cursor = conn.cursor()
# conn = odbc.connect('Driver={SQL Server};'
#                       'Server=localhost\MSSQLSERVER01;'
#                       'Database=PreProdBbrUFST;'
#                       'Trusted_Connection=yes;')
# cursor = conn.cursor()
conn = odbc.connect('Driver={SQL Server};'
                      'Server=localhost\MSSQLSERVER01;'
                      'Database=DevBbrUFST;'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()

# Create Table
cursor.execute('''
		DROP TABLE [dbo].[SourceData]
CREATE TABLE [dbo].[SourceData](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[name] [nvarchar](max) NULL,
	[arealKaelder] [int] NULL,
	[opfoerelsesAar] [int] NULL,
	[salgsPris] [int] NULL,
	[postNr] [int] NULL,
	[arealStueplan] [int] NULL,
	[kommuneNr] [int] NULL,
	[cprNr] [bigint] NULL,
	[arealGrund] [int] NULL,
	[Timestamp] [datetime] NULL,
PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

ALTER TABLE [dbo].[SourceData] ADD  DEFAULT (getdate()) FOR [Timestamp]
               ''')


# With headers
with open('C:\\Users\\JacobHøj-Kristensen\\Desktop\\tSQLt_Bbr_Test_Project\\data.csv', 'r') as f:
    reader = csv.reader(f)
    columns = next(reader)
    query = 'insert into SourceData({0}) values ({1})'
    query = query.format(','.join(columns), ','.join('?' * len(columns)))
    for data in reader:
        cursor.execute(query, data)

# without headers
# with open('C:\\Users\\JacobHøj-Kristensen\\Desktop\\tSQLt_Bbr_Test_Project\\data.csv', 'r') as f:
#     reader = csv.reader(f)
#     data = next(reader)
#     query = 'insert into SourceData values ({0})'
#     query = query.format(','.join('?' * len(data)))
#     cursor.execute(query, data)
#     for data in reader:
#         cursor.execute(query, data)



# Execute stored procedure
cursor.execute('''
		EXEC [dbo].[InsertInto_Layer1_Table1]
        EXEC [dbo].[InsertInto_Layer1_Table2]
        EXEC [dbo].[InsertInto_Layer1_Table3]

        EXEC [dbo].[InsertInto_Layer2_Table]
               ''')

try:
    cursor.commit();    
except Exception as e:
    cursor.rollback()
    print(e)
    print(str(e[1]))
finally:
    print('Task is complete.')
    cursor.close()
    conn.close()


