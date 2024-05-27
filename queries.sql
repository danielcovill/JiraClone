-- Want tickets that have been updated to 'In Progress' since <DATE> (not counting items that were blocked)
SELECT t.jira_key, t.status, MIN(h.updated) earliest_transition, h.field, h.from_val, h.to_val, h.author
FROM tickets t 
INNER JOIN history h on t.id = h.ticket_id 
WHERE t.status NOT IN ('Backlog','Selected for Development')
	AND h.field = 'status' 
	AND to_val = 'In Progress' 
	AND from_val != 'Blocked'
	AND h.updated > '2024-05-24T04:03:33.164-0700'
GROUP BY h.updated 
ORDER BY t.id