INSERT INTO raw.airport_meteostation_link
WITH airs AS (SELECT DISTINCT "AIRPORT_ID" AS airport_id, "LATITUDE" AS air_lat, "LONGITUDE" AS air_lon
				FROM raw.strike_reports sr
				WHERE "AIRPORT_ID" IS NOT NULL
				AND "LATITUDE" IS NOT NULL
				AND "LONGITUDE" IS NOT NULL),
stations AS (
			SELECT meteostation_id,
				   "LAT" AS station_lat,
				   "LON" AS station_lon
			FROM raw.isd),
cross_join AS (SELECT *,
					   SQRT(POW(69.1 * (radians(a.air_lat) -  radians(s.station_lat)), 2) +
				    POW(69.1 * (radians(s.station_lon) - radians(a.air_lon)) * COS(radians(a.air_lat) / 57.3), 2)) AS distance,
				    row_number() OVER(PARTITION BY a.airport_id ORDER BY SQRT(POW(69.1 * (radians(a.air_lat) -  radians(s.station_lat)), 2) +
				    POW(69.1 * (radians(s.station_lon) - radians(a.air_lon)) * COS(radians(a.air_lat) / 57.3), 2)) asc) AS rn
				FROM airs a
				CROSS JOIN stations s
				WHERE radians(a.air_lat) BETWEEN radians(s.station_lat) - 0.02 AND radians(s.station_lat) + 0.02)
SELECT *
FROM cross_join
WHERE rn = 1;