import pyodbc
import mysql.connector

SQLSERVER_CONN = (
   "DRIVER={ODBC Driver 18 for SQL Server};"
   "SERVER=localhost,1433;"
   "DATABASE=Credifacv1_viernes;"
   "Trusted_Connection=yes;"
   "TrustServerCertificate=yes;"
)

MYSQL_CONN = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "credifac"
}

def conectar_sqlserver():
    return pyodbc.connect(SQLSERVER_CONN)

def conectar_mysql():
    return mysql.connector.connect(**MYSQL_CONN)
