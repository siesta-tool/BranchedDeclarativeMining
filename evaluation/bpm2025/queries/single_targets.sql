SELECT 'Response',
		TaskA, 
		TaskB, 
		CAST(COUNT(*) AS FLOAT) / CAST ((SELECT COUNT(*) FROM event x WHERE x.name=TaskA) AS FLOAT) AS Support
FROM (SELECT DISTINCT a.name AS TaskA, b.name AS TaskB
	FROM event a JOIN event b ON (
		a.trace_id = b.trace_id
		AND a.name <> b.name
		AND a.time < b.time
	)
	GROUP BY a.trace_id, a.name, a.time, b.name
	) subquery
GROUP BY TaskA, TaskB

