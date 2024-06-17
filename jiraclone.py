from collections import defaultdict
from datetime import datetime
import dbcontrol
import jiraapi


db = dbcontrol.DBControl("jira.db", ".")
jira = jiraapi.JiraApi("SMART")

def main():
    while True:
        print("\n")
        try:
            print(f"Last synchronization: {db.get_last_updated_UTC().astimezone().strftime("%c")}")
            print("Choose option:")
            print("1. Synchronize local DB")
        except(AttributeError) as e:
            print("No local DB found")
            print("Choose option:")
            print("1. Initialize local DB - Full download (may take hours)")
        print('2. Get dev lead time (from "In Progress" to "Done")')
        selection = input("Return to exit...\n")

        match selection:
            case "1":
                update_database()
            case "2":
                get_development_lead_time()
                selection = 0
            case _:
                exit(0)

def update_database():
    start_time = datetime.now()
    print(f"[{start_time.strftime("%Y-%m-%d %H:%M:%S")}] Updating local database...")
    jira.sync_db()
    print(f"[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] Update complete. Run time: {str(datetime.now() - start_time)}")

def get_development_lead_time():
    lead_time = ""
    # Get tickets open between start and end dates 
    start_date = "2024-03-15T00:00:00.000-0000"
    end_date = "2024-06-15T00:00:00.000-0000"
    ticket_status_histories = db.get_ticket_status_updates(start_date, end_date)

    ticket_loiter_times = jira.get_loiter_times(ticket_status_histories)
    # TODO: Just make this a list by cycle and by month for the previous 2 years
    # start_date = input("Enter earliest date tickets completed UTC (YYYY-MM-DD): ")
    # end_date = input("Enter latest date tickets compelted UTC (YYYY-MM-DD): ")
    

    print(f"Development lead time: {lead_time}")

main()