import json
import os
from datetime import datetime
from datetime import tzinfo 
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
            response = requests.request(
                "GET",
                self.jira_url + "serverInfo",
                headers=self.request_headers,
                auth=self.request_auth
            )
            server_time = json.loads(response.text)["serverTime"]
            self.server_time_offset = datetime.fromisoformat(server_time).utcoffset()
        except:
            print("Error getting jira server info.")

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
            response = requests.request(
                "GET",
                self.jira_url + "search",
                headers=self.request_headers,
                params=query,
                auth=self.request_auth
            )
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
                response = requests.request(
                    "GET",
                    f"{self.jira_url}/issue/{ticket_id}/changelog",
                    headers = self.request_headers,
                    auth = self.request_auth,
                    params = query
                )
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