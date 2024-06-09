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
            resolved = ticket["fields"]["resolved"]
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

    # def get_ticket_status_updates(self, 
    #                               start_date, 
    #                               end_date, 
    #                               ticket_types, 
    #                               resolutions):
    #     # TODO FIX ME
    #     # select history items showing a transition to "in progress" within the filtered time window
    #     # From those history items, get the list of eligible tickets
    #     # Get all history items showing a status change relevant to those transitions 
    #     # DOWNLOAD DBEAVER
    #     if start_date is not None:
    #         start_formatted = datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
    #     if end_date is not None:
    #         end_formatted = datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y-%m-%dT23:59:59.999999+00:00")
    #     filters = []
    #     if start_date is not None:
    #         filters.append(f"(h.to_val = 'In Progress' AND h.field = 'status')")
    #     if end_date is not None:
    #         filters.append(f"(h.to_val = 'In Progress' AND h.field = 'status')")
    #     if ticket_types is not None and len(ticket_types) > 0:
    #         filters.append(f"(t.type in ({','.join(f"'{ticket_type}'" for ticket_type in ticket_types)}))")
    #     if resolutions is not None and len(resolutions) > 0:
    #         filters.append(f"(t.resolution in ({','.join(f"'{resolution}'" for resolution in resolutions)}))")
    #     query = ("SELECT t.jira_key"
	#              ", h.from_val "
    #              ", h.to_val "
    #              ", h.updated "
    #              ", t.status "
    #                 "FROM tickets t "
    #                 "INNER JOIN history h on t.id = h.ticket_id "
    #                 "WHERE h.field = 'status'"
    #                 "ORDER BY t.id DESC, h.updated DESC ")
    #              f"{" AND ".join(filters) if len(filters) > 0 else ""}")
    #     with self.dbConn:
    #         cursor = self.dbConn.cursor()
    #         cursor.execute(query)
    #         tickets = cursor.fetchall()
    #         return tickets