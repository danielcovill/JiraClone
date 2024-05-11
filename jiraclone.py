import os

import json.scanner
import dbcontrol
import requests
from requests.auth import HTTPBasicAuth
import json

db = dbcontrol.DBControl("jira.db", ".")
with open(os.path.join(".","jira_connection.json")) as jirasettingsfile:
    jirasettings = json.load(jirasettingsfile)
    url = jirasettings["url"] + "search"
    jiraUser = jirasettings["UserName"]
    jiraKey = jirasettings["ApiKey"]

auth = HTTPBasicAuth(jiraUser, jiraKey)

headers = {
  "Accept": "application/json"
}

query = {
  'jql': 'project = SMART AND updated > "2024-05-10 17:51"'
}

response = requests.request(
   "GET",
   url,
   headers=headers,
   params=query,
   auth=auth
)

print(json.dumps(json.loads(response.text), sort_keys=True, indent=4, separators=(",", ": ")))