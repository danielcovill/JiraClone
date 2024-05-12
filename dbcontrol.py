import sqlite3
import os
import json

class DBControl:

    def __init__(self,dbname,dbpath):
        self.dbName = dbname
        self.dbPath = dbpath
        self.dbConn = sqlite3.connect(
            os.path.join(self.dbPath, self.dbName)
        )
        cursor = self.dbConn.cursor()
        with self.dbConn:
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

    # Updates or inserts transactions based on thier jira id (not key) as appropriate
    def load_tickets(self, search_json):
        results = json.loads(search_json)
        rows = []

        for issue in results["issues"]:

            id = issue["id"]
            jira_key = issue["key"]
            issue_type = issue["fields"]["issuetype"]["name"]
            summary = issue["fields"]["summary"]
            created = issue["fields"]["created"]
            try:
                updated = issue["fields"]["updated"]
            except (IndexError, TypeError) as e:
                updated = None
            try:
                creator = issue["fields"]["creator"]["emailAddress"]
            except (IndexError, TypeError) as e:
                creator = None
            try:
                assignee = issue["fields"]["assignee"]["emailAddress"]
            except (IndexError, TypeError) as e:
                assignee = None
            status = issue["fields"]["status"]["name"]
            resolution = None if issue["fields"]["resolution"] == None else issue["fields"]["resolution"]
            #10026 = Story Points
            story_points = issue["fields"]["customfield_10026"]
            try:
                fix_version = issue["fields"]["fixVersions"][0]
            except (IndexError, TypeError) as e:
                fix_version = None
            #10050 = Severity
            try:
                severity = issue["fields"]["customfield_10050"]["value"]
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

        with self.dbConn:
            cursor = self.dbConn.cursor()
            cursor.executemany(sql, rows)
            cursor.close()