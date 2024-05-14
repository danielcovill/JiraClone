import sqlite3
import os
from datetime import datetime
from datetime import timezone


class DBControl:

    def __init__(self, dbname, dbpath):
        self.dbName = dbname
        self.dbPath = dbpath
        self.dbConn = sqlite3.connect(
            os.path.join(self.dbPath, self.dbName)
        )
        with self.dbConn:
            cursor = self.dbConn.cursor()
            cursor.execute("CREATE TABLE IF NOT EXISTS tickets("
                        "id PRIMARY KEY, "
                        "jira_key, "
                        "type, "
                        "summary, "
                        "created, "
                        "updated, "
                        "creator, "
                        "assignee, "
                        "status, "
                        "resolution, "
                        "story_points, "
                        "fix_version, "
                        "severity)")
            cursor.execute("CREATE TABLE IF NOT EXISTS links"
                        "(source_id UNIQUE, related_id UNIQUE, relationship)")
            cursor.execute("CREATE TABLE IF NOT EXISTS history"
                        "(id PRIMARY KEY, changetype, from_val, to_val, date)")
            cursor.execute("CREATE TABLE IF NOT EXISTS metadata"
                        "(key, val)")
            cursor.execute("SELECT * FROM metadata WHERE key = 'last_updated';")
            self.last_updated = cursor.fetchone()[1]
            # This could be because we haven't initialized the table so the key doesn't 
            # exist, or because we've never completed an update so the value is NULL. 
            # Either way, we do a reset to make sure the key is there for when we need it
            if self.last_updated == None:
                cursor.execute("DELETE FROM metadata WHERE key = 'last_updated';")
                cursor.execute("INSERT INTO metadata (key, val) VALUES ('last_updated', NULL);")


    def get_last_updated(self):
        return self.last_updated

    # Updates or inserts transactions based on thier jira id (not key) as appropriate
    def load_tickets(self, tickets):
        updateDateTime = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        rows = []

        for ticket in tickets:

            id = ticket["id"]
            jira_key = ticket["key"]
            issue_type = ticket["fields"]["issuetype"]["name"]
            summary = ticket["fields"]["summary"]
            created = ticket["fields"]["created"]
            try:
                updated = ticket["fields"]["updated"]
            except (IndexError, TypeError) as e:
                updated = None
            try:
                creator = ticket["fields"]["creator"]["emailAddress"]
            except (KeyError) as e:
                creator = ticket["fields"]["creator"]["displayName"]
            except (IndexError, TypeError) as e:
                creator = None
            try:
                assignee = ticket["fields"]["assignee"]["emailAddress"]
            except (KeyError) as e:
                assignee = ticket["fields"]["creator"]["displayName"]
            except (IndexError, TypeError) as e:
                assignee = None
            status = ticket["fields"]["status"]["name"]
            try:
                resolution = ticket["fields"]["resolution"]["name"]
            except (IndexError, TypeError) as e:
                resolution = None
            #10026 = Story Points
            story_points = ticket["fields"]["customfield_10026"]
            try:
                fix_version = ticket["fields"]["fixVersions"][0]["name"]
            except (IndexError, TypeError) as e:
                fix_version = None
            #10050 = Severity
            try:
                severity = ticket["fields"]["customfield_10050"]["value"]
            except (IndexError, TypeError) as e:
                severity = None
            rows.append({
                "id": id,
                "jira_key": jira_key,
                "issue_type": issue_type,
                "summary": summary,
                "created": created,
                "updated": updated,
                "creator": creator,
                "assignee": assignee,
                "status": status,
                "resolution": resolution,
                "story_points": story_points,
                "fix_version": fix_version,
                "severity": severity})

        sql = ("INSERT INTO tickets VALUES("
               ":id, "
               ":jira_key, "
               ":issue_type, "
               ":summary, "
               ":created, "
               ":updated, "
               ":creator, "
               ":assignee, "
               ":status, "
               ":resolution, "
               ":story_points, "
               ":fix_version, "
               ":severity) "
               "ON CONFLICT(id) DO UPDATE SET "
               "jira_key=excluded.jira_key, "
               "type=excluded.type, "
               "summary=excluded.summary, "
               "created=excluded.created, "
               "updated=excluded.updated, "
               "creator=excluded.creator, "
               "assignee=excluded.assignee, "
               "status=excluded.status, "
               "resolution=excluded.resolution, "
               "story_points=excluded.story_points, "
               "fix_version=excluded.fix_version, "
               "severity=excluded.severity")

        sql2 = (f"UPDATE metadata SET val = '{updateDateTime}' WHERE key = 'last_updated';")

        with self.dbConn:
            cursor = self.dbConn.cursor()
            cursor.executemany(sql, rows)
            cursor.execute(sql2)
            cursor.close()


    def get_tickets_without_history(self):
        with self.dbConn:
            cursor = self.dbConn.cursor()
            cursor.execute("SELECT * FROM tickets WHERE key = 'last_updated';")
            self.last_updated = cursor.fetchone()[1]

        raise NotImplementedError()