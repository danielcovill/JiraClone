import dbcontrol
import json
import os
from datetime import datetime, time
import requests
from requests.auth import HTTPBasicAuth


class JiraApi:

    def __init__(self, projectName):
        self.project_name = projectName
        self.db = dbcontrol.DBControl("jira.db", ".")

        configFilePath = os.path.join(".","jira_connection.json")
        with open(configFilePath) as jirasettingsfile:
            jirasettings = json.load(jirasettingsfile)
            self.jira_url = jirasettings["url"]
            self.jira_user = jirasettings["UserName"]
            self.jira_key = jirasettings["ApiKey"]
        self.request_auth = HTTPBasicAuth(self.jira_user, self.jira_key)
        self.request_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        try:
            response = self.request(request_type = "GET", url = self.jira_url + "serverInfo", payload=None)
        except:
            print("Failure getting server info")
            exit(1)
        server_time = json.loads(response.text)["serverTime"]
        self.server_time_offset = datetime.fromisoformat(server_time).utcoffset()

    def get_missing_ticket_ids(self):
        print("Getting missing ticket ids from Jira...")
        jql = f'project = {self.project_name}'
        all_ticket_ids = []
        morePages = True
        batchSize = 10000
        while morePages: 
            query = {
                'jql': jql,
                'startAt': len(all_ticket_ids),
                'maxResults': batchSize,
                'fields': ['id']
            }
            try:
                response = self.request("GET", url=self.jira_url + "search", payload=query)
            except:
                print("Error getting update list, exiting...")
                exit(1)
            response_json = json.loads(response.text)
            all_ticket_ids.extend([ticket["id"] for ticket in response_json["issues"]])
            if(len(all_ticket_ids)== 0):
                print("No records received")
            if len(all_ticket_ids) >= int(response_json["total"]):
                morePages = False
            print(f"{len(all_ticket_ids)} of {response_json["total"]}")
        return self.db.get_missing_tickets(all_ticket_ids)

    def sync_db(self):
        missing_ids = self.get_missing_ticket_ids()
        last_updated = self.db.get_last_updated_UTC()

        if len(missing_ids) > 0:
            missing_ids_query = f"id in ({','.join(missing_ids)})'"

        if last_updated is not None:
            last_updated_query = f"updated > '{(last_updated + self.server_time_offset).strftime("%Y-%m-%d %H:%M")}'"

        # Build the query for jira
        jql = f"project = {self.project_name}"
        if last_updated is not None and len(missing_ids) > 0:
            jql += f" AND ({last_updated_query} OR {missing_ids_query})"
        elif last_updated is None and len(missing_ids) > 0:
            jql += f" AND {missing_ids_query}"
        elif last_updated is not None and len(missing_ids) == 0:
            jql += f" AND {last_updated_query}"
        else:
            # if they're both empty, either we have an error, or it's a brand new project with no tickets
            print("No new tickets found and the db has never been updated.")
            return

        recordsReceived = 0
        morePages = True
        batchSize = 50
        print("Requesting update set from Jira...")
        while morePages: 
            query = {
                'startAt': recordsReceived,
                'maxResults': batchSize,
                'expand': ['changelog'],
                'jql': jql
            }
            response = self.request("POST", url=self.jira_url + "search", payload=query)
            response_json = json.loads(response.text)
            recordsReceived += len(response_json["issues"])
            ticketCounter = 0
            for ticket in response_json["issues"]:
                self.db.store_ticket(ticket)
                self.db.store_histories(ticket["id"], ticket["changelog"]["histories"])
                ticketCounter += 1
                if len(response_json["issues"]) < batchSize:
                    print(f"Received {recordsReceived} of "
                          f"{response_json["total"]} tickets from Jira")
                else:
                    print(f"Received {recordsReceived - (batchSize - ticketCounter)} of "
                          f"{response_json["total"]} tickets from Jira")
            if(recordsReceived == 0):
                print("No records received")
            if recordsReceived >= int(response_json["total"]):
                morePages = False    
        return

    def request(self, request_type, url, payload):
        attempt = 1
        backoff_sec = [0, .1, 1, 5]
        response = None
        while attempt <= 5:
            response = requests.request(
                request_type,
                url,
                data=json.dumps(payload),
                headers=self.request_headers,
                auth=self.request_auth
            )
            if(response.status_code == 429):
                if(attempt > 5):
                    raise Exception("Too many rate limit warnings. Ending requests.")
                print(f"Waiting {backoff_sec[attempt]} seconds to retry (attempt {attempt} of 5)...")
                time.sleep(backoff_sec[attempt])
                attempt += 1
            elif(response.status_code != 200 and response.status_code != 429):
                raise Exception("Error with request", response)
            else:
                return response