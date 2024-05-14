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
        self.auth = HTTPBasicAuth(self.jira_user, self.jira_key)
        self.headers = {
            "Accept": "application/json"
        }

        try:
            response = requests.request(
                "GET",
                self.jira_url + "serverInfo",
                headers=self.headers,
                auth=self.auth
            )
            server_time = json.loads(response.text)["serverTime"]
            self.server_time_offset = datetime.fromisoformat(server_time).utcoffset()
        except:
            print("Error getting jira server info.")

    def get_tickets_since(self, lastUpdatedUTC):
        jql = f'project = {self.project_name}'
        if lastUpdatedUTC != None:
            updatedDateTime = datetime.strptime(lastUpdatedUTC, "%Y-%m-%dT%H:%M:%S.%f%z")
            jql += f' AND updated > "{(updatedDateTime + self.server_time_offset).strftime("%Y-%m-%d %H:%M")}"'
        else:
            updatedDateTime = None
        
        print(f"Last updated: {updatedDateTime.strftime("%Y-%m-%d %H:%M:%S") if updatedDateTime != None else "Never"}")

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
                headers=self.headers,
                params=query,
                auth=self.auth
            )

            response_json = json.loads(response.text)
            recordsReceived += len(response_json["issues"])
            allRecords.extend(response_json["issues"])
            print(f"received {recordsReceived} of {response_json["total"]} records from Jira")
            if recordsReceived >= int(response_json["total"]):
                morePages = False    
        return allRecords

    def update_ticket_history(self, tickets):
        raise NotImplementedError()