import dbcontrol
import jiraapi


db = dbcontrol.DBControl("jira.db", ".")
lastUpdated = db.get_last_updated()

print(f"Last updated: {lastUpdated}")
print("Querying Jira...")
jira = jiraapi.JiraApi("SMART")
transactions = jira.get_transactions_since(lastUpdated)
print(f"Storing {len(transactions)} results in database...")
db.load_tickets(transactions)
print("Database load complete.")