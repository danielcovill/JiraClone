from datetime import datetime, timedelta, timezone
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
        print('2. Get dev teams cycle time (from "In Progress" to "Done")')
        print('3. Monthly R&I Metrics Report')
        selection = input("Return to exit...\n")

        match selection:
            case "1":
                update_database()
            case "2":
                get_development_cycle_time("2024-03-15T00:00:00.000-0000","2024-06-15T00:00:00.000-0000")
                selection = 0
            case "3":
                get_monthly_ri_metrics()
            case _:
                exit(0)

def get_monthly_ri_metrics():
    report_month = input("Enter YYYYMM for report...\n")
    if not report_month.isnumeric():
        print("Error parsing date: Enter datetime in format YYYYMM, digits only")
        return
    r_and_i_tickets_completed = db.get_r_and_i_tickets_completed(report_month)
    print(f"R&I Tickets Completed: {len(r_and_i_tickets_completed)} \n")
    r_and_i_open_tickets = jira.get_r_and_i_tickets_open(report_month)
    # TODO: Can I replace the above with the equivalent DB call?
    print(f"Count of open R&I Tickets at start of month: {len(r_and_i_open_tickets)} \n")
    r_and_i_days_outstanding = timedelta()
    counted_tickets = 0

    for ticket in r_and_i_open_tickets:
        try:
            resolved_date = datetime.strptime(ticket["fields"]["resolutiondate"], "%Y-%m-%dT%H:%M:%S.%f%z")
        except (TypeError) as e:
            resolved_date = None
        created_date = datetime.strptime(ticket["fields"]["created"],"%Y-%m-%dT%H:%M:%S.%f%z")
        # Note: this is probably wrong b/c time zone issues but close enough...
        start_of_month = datetime(int(report_month[0:4]), int(report_month[4:6]), 1, tzinfo=timezone.utc)
        if resolved_date is not None:
            open_time = min(resolved_date, start_of_month) - created_date
        else:
            open_time = start_of_month - created_date
        min_ticket_duration = timedelta(minutes=15)
        if open_time > min_ticket_duration:
            counted_tickets += 1
            r_and_i_days_outstanding += open_time

    average_r_and_i_days_outstanding = r_and_i_days_outstanding / counted_tickets
    print(f"Average days R&I Tickets outstanding at start of month: {average_r_and_i_days_outstanding} \n")

def update_database():
    start_time = datetime.now()
    print(f"[{start_time.strftime("%Y-%m-%d %H:%M:%S")}] Updating local database...")
    jira.sync_db()
    print(f"[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] Update complete. Run time: {str(datetime.now() - start_time)}")

# Assumes the items have come in grouped by key and ordered by update date
def get_transition_info(ticket_histories):
    ticket_loiter_times = {}
    current_status_date = None

    # This only works if the incoming histories are grouped by ticket 
    # and the statuses only ascend. So to make sure we catch this, we'll 
    # keep track of the current ticket and its date. If there's a 
    # deviation we'll raise an error and bail
    for ticket_history_entry in ticket_histories:
        jira_key = ticket_history_entry['jira_key']
        
        if jira_key not in ticket_loiter_times:
            ticket_loiter_times[jira_key] = {
                'last_status': None, 
                'last_update': datetime.fromisoformat(ticket_history_entry['created']),
                'first_in_progress': None
            }
            current_status_date = None

        # Check for out of order key
        if [*ticket_loiter_times.keys()][-1] != jira_key:
            raise ValueError(f"Ticket histories must be grouped by key: at {jira_key}")

        # Check for out of chronological order entries within a key 
        if current_status_date is not None and current_status_date > ticket_history_entry['updated']:
            raise ValueError(f"Ticket histories must be grouped by key and in chronological order: at {jira_key}")
        else:
            current_status_date = ticket_history_entry['updated']

        # Update the log for a given status OR make a new key with a zero timespan
        if (ticket_history_entry['field'] is not None):
            if ticket_history_entry['from_val'] not in ticket_loiter_times[jira_key]:
                ticket_loiter_times[jira_key][ticket_history_entry['from_val']] = timedelta()

            ticket_loiter_times[jira_key][ticket_history_entry['from_val']] += (
                datetime.fromisoformat(ticket_history_entry['updated']) - ticket_loiter_times[jira_key]['last_update'] 
            )
            ticket_loiter_times[jira_key]['last_status'] = ticket_history_entry['to_val']
            ticket_loiter_times[jira_key]['last_update'] = datetime.fromisoformat(ticket_history_entry['updated'])
    return ticket_loiter_times

# Cycle time starts when an issue is moved to “In Progress“ status and ends the
# last time an issue is moved to “Done” status. This function takes a start and 
# end date and prints a message giving the average cycle time for tickets within 
# that window, as well as the count of incomplete tickets.
def get_development_cycle_time(start_date, end_date):
    ticket_histories = db.get_dev_ticket_status_updates(start_date, end_date)

    # This only works if the incoming histories are grouped by ticket 
    # and the statuses only ascend. So to make sure we catch this, we'll 
    # keep track of the current ticket and its date. If there's a 
    # deviation we'll raise an error and bail
    ticket_cycle_info_dict = {}
    current_status_date = None
    prestart_statuses = ['Backlog','Selected for Development']
    for ticket_history_entry in ticket_histories:
        # skip it if the ticket isn't "started" yet
        if ticket_history_entry['status'] in prestart_statuses:
            continue

        jira_key = ticket_history_entry['jira_key']
        
        if jira_key not in ticket_cycle_info_dict:
            ticket_cycle_info_dict[jira_key] = {
                'work_start': None, 
                'work_end': None,
                'meta_projectswitch': False
            }
            current_status_date = None

        # Check for out of order key
        if [*ticket_cycle_info_dict.keys()][-1] != jira_key:
            raise ValueError(f"Ticket histories must be grouped by key: at {jira_key}")

        # Check for out of chronological order entries within a key 
        if current_status_date is not None and current_status_date > ticket_history_entry['updated']:
            raise ValueError(f"Ticket histories must be grouped by key and in chronological order: at {jira_key}")
        else:
            current_status_date = ticket_history_entry['updated']

        if ticket_history_entry['field'] is not None:
            # When tickets switch projects, we don't know what status they go into (thanks Jira API)
            # so we keep track of the date, and on the next update we find, we check the 'from_val'
            # to see where the ticket was. That way we know if this is truly the date work stated
            if ticket_history_entry['field'] in ['Key', 'Workflow']:
                ticket_cycle_info_dict[jira_key]['work_start'] = ticket_history_entry['updated']
                ticket_cycle_info_dict[jira_key]['meta_projectswitch'] = True
            elif ticket_history_entry['field'] == 'status':
                if (ticket_cycle_info_dict[jira_key]['meta_projectswitch']
                    and ticket_history_entry['from_val'] in prestart_statuses
                    and ticket_history_entry['to_val'] not in prestart_statuses):
                    ticket_cycle_info_dict[jira_key]['work_start'] = ticket_history_entry['updated']
                    ticket_cycle_info_dict[jira_key]['meta_projectswitch'] = False
                elif(not ticket_cycle_info_dict[jira_key]['meta_projectswitch']
                    and ticket_history_entry['to_val'] not in prestart_statuses
                    and ticket_cycle_info_dict[jira_key]['work_start'] is None):
                    ticket_cycle_info_dict[jira_key]['work_start'] = ticket_history_entry['updated']

                if ticket_history_entry['to_val'] == 'Done':
                    ticket_cycle_info_dict[jira_key]['work_end'] = ticket_history_entry['updated']
                else:
                    ticket_cycle_info_dict[jira_key]['work_end'] = None


    resolved_tickets = []
    total_resolved_cycle_time = timedelta()
    unresolved_tickets = []
    total_unresolved_cycle_time = timedelta()

    for ticket_id, ticket_info in ticket_cycle_info_dict.items():
        if ticket_info['work_end'] is not None:
            resolved_tickets.append(ticket_id)
            total_resolved_cycle_time += (datetime.fromisoformat(ticket_info['work_end']) - 
                                          datetime.fromisoformat(ticket_info['work_start']))
        else:
            unresolved_tickets.append(ticket_id)
            total_unresolved_cycle_time += (datetime.now(timezone.utc) - 
                                            datetime.fromisoformat(ticket_info['work_start']))

    print(f"Resolved tickets: {len(resolved_tickets)}")
    print(f"Average cycle time: {pretty_time_delta(total_resolved_cycle_time / len(resolved_tickets))}")
    print(f"Unresolved tickets: {len(unresolved_tickets)}")
    print(f"Current average unresolved ticket time: {pretty_time_delta(total_unresolved_cycle_time / len(unresolved_tickets))}")

def get_development_lead_time():
    raise NotImplementedError()
    # Get tickets open between start and end dates 
    start_date = "2024-03-15T00:00:00.000-0000" #input("Datetime in format: 2024-03-15T00:00:00.000-0000")
    end_date = "2024-06-15T00:00:00.000-0000" #input("Datetime in format: 2024-06-15T00:00:00.000-0000")
    ticket_status_histories = db.get_dev_ticket_status_updates(start_date, end_date)

    ticket_loiter_times = get_transition_info(ticket_status_histories)
    sum_of_time_per_status = {}

    for ticket_id, loiter_times in ticket_loiter_times.items():
        for status, time_in_status in loiter_times.items(): 
            if status == "last_update" or status == "last_status":
                continue
            if status not in sum_of_time_per_status:
                sum_of_time_per_status[status] = time_in_status
            else:
                sum_of_time_per_status[status] = sum_of_time_per_status[status] + time_in_status

    print(f"Development average lead time: {lead_time}")

    if input("Export lead time CSV? (y/n)") == "y":
    	with open("results.csv", "w", encoding="utf-8") as f:
            f.write("headers")
            f.writelines(ticket_loiter_times)

def pretty_time_delta(time):
    seconds = time.total_seconds()
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if days > 0:
        return '%dd %dh' % (days, hours)
    elif hours > 0:
        return '%dh %dm' % (hours, minutes)
    elif minutes > 0:
        return '%dm %ds' % (minutes, seconds)
    else:
        return '%ds' % (seconds)

main()