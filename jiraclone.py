import os

import json.scanner
import dbcontrol
import requests
from requests.auth import HTTPBasicAuth

# Set up query
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

try:
  response = requests.request(
    "GET",
    url,
    headers=headers,
    params=query,
    auth=auth
  )
except Exception as e:
    print("Error with request ")
    print(e)

db.load_tickets(response.text)