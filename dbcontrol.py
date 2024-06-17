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
        self.dbConn.row_factory = sqlite3.Row   
        with self.dbConn:
            cursor = self.dbConn.cursor()
            cursor.execute("CREATE TABLE IF NOT EXISTS tickets("
                        "id PRIMARY KEY, "
                        "jira_key, "
                        "type, "
                        "summary, "
                        "created, "
                        "resolved, "
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
                        "(id PRIMARY KEY, "
                        "ticket_id, "
                        "author, "
                        "field, "
                        "from_val, "
                        "to_val, "
                        "updated);")
            cursor.execute("CREATE TABLE IF NOT EXISTS metadata (key, val);")

            cursor.execute("SELECT count(*) FROM metadata WHERE key = 'last_updated';")
            last_updated = cursor.fetchone()
            if last_updated[0] == 0:
                cursor.execute("INSERT INTO metadata (key, val) VALUES ('last_updated', NULL);")
            self.dbConn.commit()

    def set_last_updated_UTC(self):
        update_date_sql = ("UPDATE metadata SET val = "
                        f"'{datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%z")}'"
                        " WHERE key = 'last_updated';")
        with self.dbConn:
            cursor = self.dbConn.cursor()
            cursor.execute(update_date_sql)
            self.dbConn.commit()
        

    def get_last_updated_UTC(self):
        with self.dbConn:
            cursor = self.dbConn.cursor()
            cursor.execute("SELECT * FROM metadata WHERE key = 'last_updated';")
            last_updated = cursor.fetchone()
            if last_updated[1] is not None:
                return datetime.strptime(last_updated[1], "%Y-%m-%dT%H:%M:%S.%f%z")
            else:
                return None

    # Updates or inserts transactions based on thier jira id (not key) as appropriate
    def store_tickets(self, tickets):
        ticket_sql = ("INSERT INTO tickets VALUES ("
            ":id, "
            ":jira_key, "
            ":issue_type, "
            ":summary, "
            ":created, "
            ":resolved, "
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
            "resolved=excluded.resolved, "
            "updated=excluded.updated, "
            "creator=excluded.creator, "
            "assignee=excluded.assignee, "
            "status=excluded.status, "
            "resolution=excluded.resolution, "
            "story_points=excluded.story_points, "
            "fix_version=excluded.fix_version, "
            "severity=excluded.severity, "
            "sync_date=sync_date")
        ticket_rows = []
        history_sql = ("INSERT INTO history VALUES ("
                ":id, "
                ":ticket_id, "
                ":author, "
                ":field, "
                ":from_val, "
                ":to_val, "
                ":updated) "
                "ON CONFLICT (id) DO UPDATE SET "
                "ticket_id=excluded.ticket_id, "
                "author=excluded.author, "
                "field=excluded.field, "
                "from_val=excluded.from_val, "
                "to_val=excluded.to_val, "
                "updated=excluded.updated")
        history_rows = []
        for ticket in tickets:
            ticket_id = ticket["id"]
            jira_key = ticket["key"]
            issue_type = ticket["fields"]["issuetype"]["name"]
            summary = ticket["fields"]["summary"]
            created = ticket["fields"]["created"]
            resolved = ticket["fields"]["resolutiondate"]
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
            ticket_rows.append({
                "id": ticket_id,
                "jira_key": jira_key,
                "issue_type": issue_type,
                "summary": summary,
                "created": created,
                "resolved": resolved,
                "updated": updated,
                "creator": creator,
                "assignee": assignee,
                "status": status,
                "resolution": resolution,
                "story_points": story_points,
                "fix_version": fix_version,
                "severity": severity,
                "sync_date": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%z")})

            for history_entry in ticket["changelog"]["histories"]:
                try:
                    author = history_entry["author"]["emailAddress"]
                except (KeyError) as e:
                    author = history_entry["author"]["displayName"]
                except (IndexError, TypeError) as e:
                    author = None
                updated = history_entry["created"]
                for changed_value in history_entry["items"]:
                    history_id = history_entry["id"]
                    field = changed_value["field"]
                    from_val = changed_value["fromString"]
                    to_val = changed_value["toString"]
                    history_rows.append({
                        "id": history_id,
                        "ticket_id": ticket_id,
                        "author": author,
                        "field": field,
                        "from_val": from_val,
                        "to_val": to_val,
                        "updated": updated
                    })
        with self.dbConn:
            cursor = self.dbConn.cursor()
            cursor.executemany(ticket_sql, ticket_rows)
            cursor.executemany(history_sql, history_rows)
            self.dbConn.commit()

    def get_all_ticket_ids(self):
        with self.dbConn:
            cursor = self.dbConn.cursor()
            cursor.execute(f"SELECT id FROM tickets;")
            db_ticket_ids = [id[0] for id in cursor.fetchall()]
            self.dbConn.commit()
        return db_ticket_ids

    # Get a list of tickets with status updates for any tickets that were open at any 
    # point within a time window. Dates must be formatted as %Y-%m-%dT%H:%M:%S.%f+00:00
    # History entries will be returned in descending order of ticket id, then ascending
    # updated date. Empty history entries just get a single row with ticket and nulls
    def get_ticket_status_updates(self, 
                                  start_date, 
                                  end_date):
        if start_date is None:
            start_date = '2020-01-01T00:00:00.000-0000'
        if end_date is None:
            end_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%z")

        query = (
            "SELECT t.jira_key"
            ", h.field "
            ", h.from_val "
            ", h.to_val "
            ", h.updated "
            ", t.created "
            ", t.status "
            ", t.resolved "
            ", t.resolution "
            "FROM tickets t "
            "LEFT JOIN history h on t.id = h.ticket_id AND h.field = 'status'"
            "WHERE t.\"type\" IN ('Bug','Story','Task')"
            "AND t.status != 'Backlog' " # nothing we haven't even started
            f"AND t.created <= '{end_date}' "# created prior to end window
            "AND ( "
                    # we only care about stuff we decided to do (E.g., not duplicates)
                    f"(t.resolved >= '{end_date}' AND t.resolution IN ('Done', 'Cannot Reproduce')) "
                    "OR "
                    "t.status != 'Done' "
	            ") "
            "ORDER BY t.id DESC, h.updated ASC")
        with self.dbConn:
            cursor = self.dbConn.cursor()
            cursor.execute(query)
            ticket_histories = cursor.fetchall()
            return ticket_histories