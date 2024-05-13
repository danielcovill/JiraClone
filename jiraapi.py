import json
import os
from datetime import datetime
import requests
from requests.auth import HTTPBasicAuth


class JiraApi:

    def __init__(self, projectName):
        self.project_name = projectName

        configFilePath = os.path.join(".","jira_connection.json")
        with open(configFilePath) as jirasettingsfile:
            jirasettings = json.load(jirasettingsfile)
            self.jira_url = jirasettings["url"] + "search"
            self.jira_user = jirasettings["UserName"]
            self.jira_key = jirasettings["ApiKey"]

    def get_tickets_since(self, lastUpdated):
        auth = HTTPBasicAuth(self.jira_user, self.jira_key)

        headers = {
            "Accept": "application/json"
        }

        jql = f'project = {self.project_name}'
        try:
            updatedDateTime = datetime.strptime(lastUpdated, "%Y-%m-%dT%H:%M:%S.%f%z")
            # TODO Figure out how to handle the time zone properly here
            jql += f' AND updated > "{updatedDateTime.strftime("%Y-%m-%d %H:%M")}"'
        except:
            updatedDateTime = None
        
        print(f"Last updated: {updatedDateTime.strftime("%Y-%m-%d %H:%M:%S") if updatedDateTime != None else "Never"}")

        recordsReceived = 0
        morePages = True

        # TODO: store the records as we go, then refactor the DB code to just take the records json
        allRecords = []

        while morePages: 
            query = {
                'jql': jql,
                'startAt': recordsReceived,
                'maxResults': 50
            }

            response = requests.request(
                "GET",
                self.jira_url,
                headers=headers,
                params=query,
                auth=auth
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