__author__ = 'TARSIER'

import MySQLdb
'''
MySQLdb User Guide  : http://mysqlclient.readthedocs.io/user_guide.html
MySQLdb Source      : https://github.com/PyMySQL/mysqlclient-python
'''


class MyDB(object):
    '''
    A simple mysql client helper for python associated from MySQLdb
    '''
    def __init__(self, host, user, password, database):
        self.db_host = host
        self.db_user = user
        self.db_password = password
        self.db_name = database

        self.open()

    def open(self):
        try:
            # Open database connection
            self.db_conn = MySQLdb.connect(self.db_host, self.db_user, self.db_password, self.db_name)
        except:
            pass

    def close(self):
        if self.db_conn.open:
            # disconnect from server
            self.db_conn.close()

    def executeQuery(self, query):
        if query and query.strip() is not None:
            # prepare a cursor object using cursor() method
            self.cursor = self.db_conn.cursor()
            # execute SQL query using execute() method.
            self.cursor.execute(query)
        else:
            pass

    def getVersion(self):
        if self.db_conn.open:
            self.executeQuery("SELECT VERSION()")

            # Fetch a single row using fetchone() method.
            data = self.cursor.fetchone()
            # print "Database version : %s " % data
            return data
        else:
            return "Database cannot get version details because DB was closed."

    def createTable(self, table, fields):
        if table and table.strip() is not None:
            sql = []
            cols = []
            sql.append('CREATE TABLE IF NOT EXISTS ' + table + ' (')
            for f, v in fields.items():
                cols.append((str(f) + ' ' + str(v)))

            columns = ",".join(cols)
            columns = columns.strip()
            sql.append(columns)
            sql.append(');')
            sql = "".join(sql)
            self.executeQuery(sql)

    def getAllTables(self, database):
        self.executeQuery("SHOW FULL TABLES FROM "+ str(database))
        return self.cursor.fetchall()

    def getAllTableColumns(self, table):
        self.executeQuery("SHOW COLUMNS FROM "+ str(table))
        return self.cursor.fetchall()

    def getAllTableRows(self, table):
        self.executeQuery("SELECT * FROM "+ str(table))
        return self.cursor.fetchall()

def main():
    databaseName = 'information_schema'
    mdbase = MyDB('127.0.0.1', 'root', 'password', databaseName)
    print (mdbase.getVersion())

    results = mdbase.getAllTables(databaseName)
    for row in results:
        print (row)



if __name__ == '__main__':
    main()
