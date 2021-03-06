import pypyodbc as odbc  
import pandas as pd
import csv  

# Import data from csv file
data = pd.read_csv('C:\\Users\\JacobHøj-Kristensen\\Desktop\\tSQLt_Bbr_Test_Project\\data.csv')
df = pd.DataFrame(data)
print(df)

## Step 3.1 Create SQL Servre Connection String

# Connect to SQL Server
conn = odbc.connect('Driver={SQL Server};'
                      'Server=localhost\MSSQLSERVER01;'
                      'Database=DevBbrUFST;'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()

# Create Table
cursor.execute('''
		drop table [dbo].[SourceData]
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
	[Timestamp] [datetime] NULL,
PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

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


# Execute stored procedure
cursor.execute('''
		EXEC [dbo].[InsertInto_Lag1_Table1]
        EXEC [dbo].[InsertInto_Lag1_Table2]
        EXEC [dbo].[InsertInto_Lag1_Table3]

        EXEC [dbo].[InsertInto_Lag2_Tabel]
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


