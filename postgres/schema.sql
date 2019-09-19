import psycopg2

connection = psycopg2.connect(host = '127.0.0.1', database = 'test', user = 'postgres', password = 'new')

cursor = connection.cursor()

cursor.execute('''INSERT INTO playground (equip_id, type, color, location) values (0, 'see-saw', 'red', 'north')''')

connection.commit() # Important!

cursor.execute('''SELECT * FROM playground''')

print(cursor.fetchall())

cursor.close()
connection.close()
