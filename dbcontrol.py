import sqlite3
import os

class DBControl:
    def __init__(self,dbname,dbpath):
        self.dbName = dbname
        self.dbPath = dbpath
        self.dbConn = sqlite3.connect(
            os.path.join(self.dbPath, self.dbName)
        )
        cursor = self.dbConn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS ticket(id, type, description, created, updated, creator, assignee, status, resolution, points, fixVersion, severity)")
        cursor.execute("CREATE TABLE IF NOT EXISTS links(source_id, related_id, relationship)")
        cursor.execute("CREATE TABLE IF NOT EXISTS history(id, changetype, from_val, to_val, date)")