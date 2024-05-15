import dbcontrol
import jiraapi


db = dbcontrol.DBControl("jira.db", ".")
lastUpdatedUTC = db.get_last_updated_UTC()

print("Querying Jira for new tickets...")
print(f"Last updated: {lastUpdatedUTC}")
jira = jiraapi.JiraApi("SMART")
tickets = jira.get_tickets_since_UTC(lastUpdatedUTC)

print(f"Storing {len(tickets)} tickets in database...")
db.store_tickets(tickets)

print("Querying Jira for ticket histories...")
# Getting the tickets without history after the insert means
# we also catch any random ones that need cleanup
ticket_ids = db.get_ticket_ids_without_history()
histories = jira.get_tickets_histories(ticket_ids)

print(f"Updating {len(histories)} history entries in database...")
db.store_histories(histories)

print("Update complete")