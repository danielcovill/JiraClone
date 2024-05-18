from datetime import datetime
import dbcontrol
import jiraapi


db = dbcontrol.DBControl("jira.db", ".")

start_time = datetime.now()
print(f"[{start_time.strftime("%Y-%m-%d %H:%M:%S")}] Updating local database...")
jira = jiraapi.JiraApi("SMART")
jira.sync_db()
print(f"[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] Update complete. Run time: {str(datetime.now() - start_time)}")