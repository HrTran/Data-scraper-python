"""
author: huytt99
created date: 19/07/19
"""
import mysql.connector
from mysql.connector import MySQLConnection, Error


def hello(name):
    print("Hello, " + name)


def executeQuery(host, database, user, password, query):
    try:
        connection = mysql.connector.connect(host=host,
                                             database=database,
                                             user=user,
                                             password=password)
        sql_insert_query = query
        cursor = connection.cursor()
        result = cursor.execute(sql_insert_query)
        connection.commit()
        print("Record inserted successfully into python_users table")
    except mysql.connector.Error as error :
        connection.rollback()  # rollback if any exception occured
        print("Failed inserting record into python_users table {}".format(error))
    finally:
        # closing database connection.
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")
