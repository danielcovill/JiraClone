import dbcontrol
import jiraapi


db = dbcontrol.DBControl("jira.db", ".")
lastUpdatedUTC = db.get_last_updated()

print(f"Last updated: {lastUpdatedUTC}")
print("Querying Jira for new tickets...")
jira = jiraapi.JiraApi("SMART")
tickets = jira.get_tickets_since(lastUpdatedUTC)
print(f"Storing {len(tickets)} tickets in database...")
db.load_tickets(tickets)
print("Store complete...")
print("Loading ticket history data...")
tickets_needing_history = db.get_tickets_without_history()
# TODO: Merge is with the list of updated tickets, who will have new history to record
jira.update_ticket_history(tickets_needing_history)
print("Ticket history update complete")