import MySQLCredentials as mc
import mysql.connector
connection = mysql.connector.connect(
    user = mc.user,
    password = mc.password,
    host = mc.host,
    database = mc.database
    )
cursor = connection.cursor()