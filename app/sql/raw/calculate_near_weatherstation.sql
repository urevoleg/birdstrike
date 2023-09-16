WITH airs AS (SELECT DISTINCT "AIRPORT_ID" AS airport_id, "LATITUDE" AS lat, "LONGITUDE" AS lon
				FROM public.strike_reports sr
				WHERE "AIRPORT_ID" IS NOT NULL),
stations AS (
			SELECT meteostation_id,
				   "LAT" AS lat,
				   "LON" AS lon
			FROM isd),
cross_join AS (SELECT *,
					   SQRT(POW(69.1 * (a.lat::float -  s.lat::float), 2) +
				    POW(69.1 * (s.lon::float - a.lon::float) * COS(a.lat::float / 57.3), 2)) AS distance,
				    row_number() OVER(PARTITION BY a.airport_id ORDER BY SQRT(POW(69.1 * (a.lat::float -  s.lat::float), 2) +
				    POW(69.1 * (s.lon::float - a.lon::float) * COS(a.lat::float / 57.3), 2)) asc) AS rn
				FROM airs a
				CROSS JOIN stations s)
SELECT *
FROM cross_join
WHERE rn = 1;