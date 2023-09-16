WITH links AS (SELECT aml.airport_id,
					  aml.meteostation_id
				FROM raw.airport_meteostation_link aml),
strike AS (SELECT s."INDEX_NR",
				  s."AIRPORT_ID" AS airport_id,
				  s."INCIDENT_DATE" AS incidented_at
			FROM raw.strike_reports s
			WHERE s."INCIDENT_DATE" > '2018-01-01'),
weather AS (SELECT wn."STATION" AS meteostation_id,
				   wn."DATE" AS dated_at
			FROM raw.weather_noaa wn)
SELECT s.airport_id,
	   s.incidented_at,
	   s."INDEX_NR",
	   w.dated_at
FROM strike s
LEFT JOIN links l
USING (airport_id)
LEFT JOIN weather w
ON s.incidented_at = w.dated_at AND l.meteostation_id = w.meteostation_id;