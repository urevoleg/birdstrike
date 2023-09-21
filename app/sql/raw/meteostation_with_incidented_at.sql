WITH links AS (SELECT aml.airport_id,
					  aml.meteostation_id
				FROM raw.airport_meteostation_link aml),
strike AS (SELECT s."INDEX_NR",
				  s."AIRPORT_ID" AS airport_id,
				  to_timestamp(concat("INCIDENT_DATE"::date::TEXT, ' ', "TIME"), 'YYYY-MM-DD HH24:MI') AT TIME ZONE 'UTC' AS raw_incidented_at,
				  date_trunc('hour', to_timestamp(concat("INCIDENT_DATE"::date::TEXT, ' ', "TIME"), 'YYYY-MM-DD HH24:MI') AT TIME ZONE 'UTC' + INTERVAL '30minute') AS incidented_at
			FROM raw.strike_reports s
			WHERE s."INCIDENT_DATE" > '2018-01-01'
			AND "TIME" SIMILAR TO '%((0|1)(0|1|2|3|4|5|6|7|8|9):(0|1|2|3|4|5)(0|1|2|3|4|5|6|7|8|9))|2(0|1|2|3):(0|1|2|3|4|5)(0|1|2|3|4|5|6|7|8|9)%')
SELECT s."INDEX_NR",
	   l.meteostation_id,
	   s.raw_incidented_at,
	   s.raw_incidented_at - interval '30 minute' AS started_at,
       s.raw_incidented_at + interval '30 minute' AS ended_at
FROM strike s
JOIN links l
USING (airport_id)
where s.raw_incidented_at > :dated_at
ORDER BY s.raw_incidented_at;