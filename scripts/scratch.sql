SELECT YEAR(EVT_TIMESTAMP) yearss,
	MONTH(EVT_TIMESTAMP) monthss,
	DAY(EVT_TIMESTAMP) dayss,
	HOUR(EVT_TIMESTAMP) hourss,
	COUNT(*) countstar
FROM EQUIPMENT ip
WHERE DEVICEID = '73001'
GROUP BY
	YEAR(EVT_TIMESTAMP),
	MONTH(EVT_TIMESTAMP),
	DAY(EVT_TIMESTAMP),
	HOUR(EVT_TIMESTAMP)
ORDER BY
	YEAR(EVT_TIMESTAMP) DESC,
	MONTH(EVT_TIMESTAMP) DESC,
	DAY(EVT_TIMESTAMP) DESC,
	HOUR(EVT_TIMESTAMP) DESC

