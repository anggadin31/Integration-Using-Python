import gspread
import MySQLCredentials as mc
import mysql.connector
from oauth2client.service_account import ServiceAccountCredentials

class WriteToSQL():
    def __init__(self):
        self.scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
        self.creds = ServiceAccountCredentials.from_json_keyfile_name('secret.json', self.scope)
        self.client = gspread.authorize(self.creds)
        self.values = []
    
    def GetSpreadsheetData(self):
        # Get all spreadsheets
        for spreadsheet in self.client.openall():
            # Get spreadsheet's worksheets
            worksheets = spreadsheet.worksheets()
            for ws in worksheets:
                # Append the values of the worksheet to values
                self.values.append(ws.get_all_values()[1:])
        return self.values
    
    def checkTableExists(self, dbcon, tablename):
        dbcur = dbcon.cursor()
        dbcur.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(tablename.replace('\'', '\'\'')))
        if dbcur.fetchone()[0] == 1:
            dbcur.close()
            return True
        dbcur.close()
        return False
    
    def WriteToMySQLTable(self, sql_data, tableName):
        try:
            connection = mysql.connector.connect(
                user = mc.user,
                password = mc.password,
                host = mc.host,
                database = mc.database
                )
            exists = self.checkTableExists(connection,tablename=tableName)
            cursor = connection.cursor()
            if exists == True:
                sql_insert_statement = """INSERT INTO {}( 
                Angka,
                Huruf
                )
                VALUES ( %s,%s )""".format(tableName)
                for i in range(len(sql_data)):
                    for j in range(len(sql_data[i])):
                        cursor.execute(sql_insert_statement, sql_data[i][j])
            else:
                sql_create_table = """CREATE TABLE {}( 
                Angka INT(11),
                Huruf VARCHAR(30),
                PRIMARY KEY (Angka)
                )""".format(tableName)  
                cursor.execute(sql_create_table)
                print('Table {} has been created'.format(tableName))
                sql_insert_statement = """INSERT INTO {}( 
                Angka,
                Huruf
                )
                VALUES ( %s,%s )""".format(tableName)
                for i in range(len(sql_data)):
                    for j in range(len(sql_data[i])):
                        cursor.execute(sql_insert_statement, sql_data[i][j])
            connection.commit()
            print("Table {} successfully updated.".format(tableName))
        except mysql.connector.Error as error :
            connection.rollback()
            print("Error: {}. Table {} not updated!".format(error, tableName))
        finally:
            cursor.execute('SELECT COUNT(*) FROM {}'.format(tableName))
            rowCount = cursor.fetchone()[0]
            print(tableName, 'row count:', rowCount)
            if connection.is_connected():
                cursor.close()
                connection.close()
                print("MySQL connection is closed.")

script = WriteToSQL()
SheetData = script.GetSpreadsheetData()
SqlQuery = script.WriteToMySQLTable(SheetData,"contoh")