INSERT INTO raw.airport_meteostation_link
WITH airs AS (SELECT DISTINCT "AIRPORT_ID" AS airport_id, "LATITUDE" AS air_lat, "LONGITUDE" AS air_lon
				FROM raw.strike_reports sr
				WHERE "AIRPORT_ID" IS NOT NULL),
stations AS (
			SELECT meteostation_id,
				   "LAT" AS station_lat,
				   "LON" AS station_lon
			FROM raw.isd),
cross_join AS (SELECT *,
					   SQRT(POW(69.1 * (a.air_lat::float -  s.station_lat::float), 2) +
				    POW(69.1 * (s.station_lon::float - a.air_lon::float) * COS(a.air_lat::float / 57.3), 2)) AS distance,
				    row_number() OVER(PARTITION BY a.airport_id ORDER BY SQRT(POW(69.1 * (a.air_lat::float -  s.station_lat::float), 2) +
				    POW(69.1 * (s.station_lon::float - a.air_lon::float) * COS(a.air_lat::float / 57.3), 2)) asc) AS rn
				FROM airs a
				CROSS JOIN stations s)
SELECT *
FROM cross_join
WHERE rn = 1;