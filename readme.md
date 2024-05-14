# TimeZones 
Tracking of when updates happen is recorded in the local sqlite3 DB metadata table using UTC.

Tickets returned from the Jira API automatically have the "local user" time zone applied for the created and updated fields
* https://developer.atlassian.com/cloud/jira/platform/rest/v3/intro/#timestamps

When querying, search results will be relative to your configured time zone (which is by default the Jira server's time zone). When querying the JQL endpoint, you may not provide a timezone offset.
* https://support.atlassian.com/jira-software-cloud/docs/jql-fields/

The consequence of the above is that when querying we must first check the server offset, and then apply it to our request so that we're using "local time" instead of UTC.