import sqlite3
import os
from datetime import datetime
from datetime import timezone


class DBControl:

    def __init__(self, dbname, dbpath):
        self.dbName = dbname
        self.dbPath = dbpath
        self.dbConn = sqlite3.connect(
            os.path.join(self.dbPath, self.dbName), 
            autocommit=False
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
                        "severity, "
                        "sync_date);")
            cursor.execute("CREATE TABLE IF NOT EXISTS history"
                        "(ticket_id, author, field, from_val, to_val, updated);")
            cursor.execute("CREATE TABLE IF NOT EXISTS metadata (key, val);")

            cursor.execute("SELECT * FROM metadata WHERE key = 'last_updated';")
            last_updated = cursor.fetchone()
            if last_updated is None:
                # This could be because we haven't initialized the table so the key doesn't 
                # exist, or because we've never completed an update so the value is NULL. 
                # Either way, we do a reset to make sure the key is there for when we need it
                cursor.execute("INSERT INTO metadata (key, val) VALUES ('last_updated', NULL);")

            self.dbConn.commit()


    def get_last_updated_UTC(self):
        with self.dbConn:
            cursor = self.dbConn.cursor()
            cursor.execute("SELECT * FROM metadata WHERE key = 'last_updated';")
            last_updated = cursor.fetchone()
            if last_updated is not None:
                return last_updated[1]
            else:
                return None

    # Updates or inserts transactions based on thier jira id (not key) as appropriate
    def store_ticket(self, ticket):
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
        toAdd = {
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
            "severity": severity,
            "sync_date": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%z")}

        sql = ("INSERT INTO tickets VALUES ("
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
            ":severity, "
            ":sync_date) "
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
            "severity=excluded.severity, "
            "sync_date=sync_date")

        history_rows = []
        for history_entry in ticket["changelog"]["histories"]:
            try:
                author = history_entry["author"]["emailAddress"]
            except (KeyError) as e:
                author = history_entry["author"]["displayName"]
            except (IndexError, TypeError) as e:
                author = None
            updated = history_entry["created"]
            for changed_value in history_entry["items"]:
                field = changed_value["field"]
                from_val = changed_value["fromString"]
                to_val = changed_value["toString"]
                history_rows.append({
                    "ticket_id": id,
                    "author": author,
                    "field": field,
                    "from_val": from_val,
                    "to_val": to_val,
                    "updated": updated
                })
            sql = ("INSERT INTO history VALUES("
                ":ticket_id, "
                ":author, "
                ":field, "
                ":from_val, "
                ":to_val, "
                ":updated)")

        with self.dbConn:
            cursor = self.dbConn.cursor()
            cursor.execute(sql, toAdd)
            cursor.executemany(sql, history_rows)
            self.dbConn.commit()

    def get_missing_tickets(self, jira_ticket_ids):
        with self.dbConn:
            cursor = self.dbConn.cursor()
            cursor.execute(f"SELECT id FROM tickets;")
            db_ticket_ids = [id[0] for id in cursor.fetchall()]
            missing = set(jira_ticket_ids).difference(db_ticket_ids)
            self.dbConn.commit()
        return missing