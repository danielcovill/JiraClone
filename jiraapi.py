import json
import os
from datetime import datetime, time
import requests
from requests.auth import HTTPBasicAuth


class JiraApi:

    def __init__(self, projectName):
        self.project_name = projectName

        configFilePath = os.path.join(".","jira_connection.json")
        with open(configFilePath) as jirasettingsfile:
            jirasettings = json.load(jirasettingsfile)
            self.jira_url = jirasettings["url"]
            self.jira_user = jirasettings["UserName"]
            self.jira_key = jirasettings["ApiKey"]
        self.request_auth = HTTPBasicAuth(self.jira_user, self.jira_key)
        self.request_headers = {
            "Accept": "application/json"
        }
        try:
            response = self.get_request(url = self.jira_url + "serverInfo", params=None)
        except:
            print("Failure getting server info")
            exit(1)
        server_time = json.loads(response.text)["serverTime"]
        self.server_time_offset = datetime.fromisoformat(server_time).utcoffset()

    def get_tickets_since_UTC(self, lastUpdatedUTC):
        jql = f'project = {self.project_name}'
        if lastUpdatedUTC != None:
            updatedDateTime = datetime.strptime(lastUpdatedUTC, "%Y-%m-%dT%H:%M:%S.%f%z")
            jql += f' AND updated > "{(updatedDateTime + self.server_time_offset).strftime("%Y-%m-%d %H:%M")}"'
        else:
            updatedDateTime = None
        
        recordsReceived = 0
        morePages = True
        allRecords = []

        while morePages: 
            query = {
                'jql': jql,
                'startAt': recordsReceived,
                'maxResults': 50
            }
            try:
                response = self.get_request(url=self.jira_url + "search", params=query)
            except:
                print("Error getting tickets, exiting...")
                exit(1)
            response_json = json.loads(response.text)
            recordsReceived += len(response_json["issues"])
            allRecords.extend(response_json["issues"])
            if(recordsReceived == 0):
                print("No records received")
            else:
                print(f"Received {recordsReceived} of {response_json["total"]} "
                    "tickets from Jira")
            if recordsReceived >= int(response_json["total"]):
                morePages = False    
        return allRecords

    def get_tickets_histories(self, ticket_ids):
        allRecords = []
        ticket_index = 1
        for ticket_id in ticket_ids:
            ticket_history = []
            recordsReceived = 0
            morePages = True
            while morePages: 
                query = {
                    'startAt': recordsReceived,
                    'maxResults': 50
                }
                try:
                    response = self.get_request(url=f"{self.jira_url}/issue/{ticket_id}/changelog", params=query)
                except:
                    print("Error getting ticket histories, exiting...")
                    exit(1)
                response_json = json.loads(response.text)
                recordsReceived += len(response_json["values"])
                ticket_history.extend(response_json["values"])
                if recordsReceived >= int(response_json["total"]):
                    morePages = False    
                    allRecords.append({'ticket_id': ticket_id, 'history': ticket_history})
                    print(f"Received history for ticket {ticket_index} of {len(ticket_ids)}")
                    ticket_index += 1
                else:
                    print(f"Received history items {recordsReceived} of {response_json["total"]}"
                          f" for ticket {ticket_index} of {len(ticket_ids)}")
        return allRecords

    def get_request(self, url, params):
        attempt = 1
        backoff_sec = [0, .1, 1, 5]
        response = None
        while attempt <= 5 and (response == None or not response.ok):
            try:
                response = requests.request(
                    "GET",
                    url = url,
                    headers=self.request_headers,
                    auth=self.request_auth,
                    params=params
                )
            except (ConnectionError) as e:
                if(attempt > 5):
                    raise e
                print(e.strerror)
                print(f"Waiting {backoff_sec[attempt]} seconds to retry (attempt {attempt} of 5)...")
                time.sleep(backoff_sec[attempt])
                attempt += 1
        return response