import dbcontrol
import jiraapi


db = dbcontrol.DBControl("jira.db", ".")

print("Updating local database...")
jira = jiraapi.JiraApi("SMART")
jira.sync_db()
print("Update complete")