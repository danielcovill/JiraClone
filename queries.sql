SELECT t.jira_key
	, h.field 
	, h.from_val 
	, h.to_val 
	, h.updated
	, t.created 
	, t.status 
	, t.resolved 
	, t.resolution 
FROM tickets t 
LEFT JOIN history h on t.id = h.ticket_id AND h.field = 'status'
WHERE t."type" IN ('Bug','Story','Task')
AND t.status != 'Backlog' --nothing we haven't even started
AND t.created <= '2024-06-15T00:00:00.000-0600'--created prior to end window
AND (
		-- we only care about stuff we decided to do (E.g., not duplicates)
		(t.resolved >= '2024-03-15T00:00:00.000-0600' AND t.resolution IN ('Done', 'Cannot Reproduce'))
		OR 
		t.status != 'Done'
	)
ORDER BY t.id DESC, h.updated ASC