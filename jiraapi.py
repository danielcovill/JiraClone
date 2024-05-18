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
            response = self.request(
                request_type = "GET", 
                url = self.jira_url + "serverInfo", 
                payload=None)
        except:
            print("Failure getting server info")
            exit(1)
        server_time = json.loads(response.text)["serverTime"]
        self.server_time_offset = datetime.fromisoformat(server_time).utcoffset()

    def sync_db(self):
        existing_ids = self.db.get_all_ticket_ids()
        last_updated = self.db.get_last_updated_UTC()

        if last_updated is None or len(existing_ids) == 0:
            jql = f"project = {self.project_name}"
        else:
            last_updated_server_time = (last_updated + self.server_time_offset).strftime("%Y-%m-%d %H:%M")
            jql = (f'project = {self.project_name} AND '
                   f'(id NOT IN ({','.join(existing_ids)}) '
                   f' OR (updated > "{last_updated_server_time}" )'
                   f'and project = "{self.project_name}")')

        records_received = 0
        morePages = True
        batchSize = 1000
        print("Requesting update set from Jira...")
        while morePages: 
            query = {
                'startAt': records_received,
                'maxResults': batchSize,
                'expand': ['changelog'],
                'jql': jql
            }
            response = self.request("POST", url=self.jira_url + "search", payload=query)
            response_json = json.loads(response.text)
            self.db.store_tickets(response_json["issues"])
            records_received += len(response_json["issues"])
            print(f"Stored {records_received} of {response_json["total"]} "
                  f"({(records_received/int(response_json["total"])):.0%})")
            if(records_received == 0):
                print("No records received")
            if records_received >= int(response_json["total"]):
                morePages = False    

        self.db.set_last_updated_UTC()
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