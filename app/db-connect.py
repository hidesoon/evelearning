#!/usr/bin/python3

import pymysql

# Open database connection
db = pymysql.connect(host="localhost",user="root",passwd="1234567890" )

# prepare a cursor object using cursor() method
cursor = db.cursor()

# execute SQL query using execute() method.
cursor.execute("SELECT VERSION()")

# Fetch a single row using fetchone() method.
data = cursor.fetchone()
print ("Database version : %s " % data)

# disconnect from server
db.close()