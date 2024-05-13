import dbcontrol
import jiraapi


db = dbcontrol.DBControl("jira.db", ".")
lastUpdated = db.get_last_updated()

print(f"Last updated: {lastUpdated}")
print("Querying Jira for new tickets...")
jira = jiraapi.JiraApi("SMART")
tickets = jira.get_tickets_since(lastUpdated)
print(f"Storing {len(tickets)} tickets in database...")
db.load_tickets(tickets)
print("Ticket update complete, records stored in local database.")
print("Loading ticket history for missing tickets...")
jira.update_ticket_history()