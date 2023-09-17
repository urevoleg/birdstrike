create schema raw;
-- raw.weather_noaa definition

-- Drop table

-- DROP TABLE raw.weather_noaa;

CREATE TABLE raw.weather_noaa (
	"STATION" text NULL,
	"DATE" timestamp NULL,
	"SOURCE" text NULL,
	"LATITUDE" float8 NULL,
	"LONGITUDE" float8 NULL,
	"ELEVATION" float8 NULL,
	"NAME" text NULL,
	"REPORT_TYPE" text NULL,
	"CALL_SIGN" text NULL,
	"QUALITY_CONTROL" text NULL,
	"WND" text NULL,
	"CIG" text NULL,
	"VIS" text NULL,
	"TMP" text NULL,
	"DEW" text NULL,
	"SLP" text NULL
);

-- raw.strike_reports definition

-- Drop table

-- DROP TABLE raw.strike_reports;

CREATE TABLE raw.strike_reports (
	"index" int8 NULL,
	"INDEX_NR" int8 NULL,
	"INCIDENT_DATE" timestamp NULL,
	"INCIDENT_MONTH" int8 NULL,
	"INCIDENT_YEAR" int8 NULL,
	"TIME" text NULL,
	"TIME_OF_DAY" text NULL,
	"AIRPORT_ID" text NULL,
	"AIRPORT" text NULL,
	"LATITUDE" float8 NULL,
	"LONGITUDE" float8 NULL,
	"RUNWAY" text NULL,
	"STATE" text NULL,
	"FAAREGION" text NULL,
	"LOCATION" text NULL,
	"OPID" text NULL,
	"OPERATOR" text NULL,
	"REG" text NULL,
	"FLT" text NULL,
	"AIRCRAFT" text NULL,
	"AMA" text NULL,
	"AMO" text NULL,
	"EMA" float8 NULL,
	"EMO" float8 NULL,
	"AC_CLASS" text NULL,
	"AC_MASS" float8 NULL,
	"TYPE_ENG" text NULL,
	"NUM_ENGS" float8 NULL,
	"ENG_1_POS" float8 NULL,
	"ENG_2_POS" float8 NULL,
	"ENG_3_POS" float8 NULL,
	"ENG_4_POS" float8 NULL,
	"PHASE_OF_FLIGHT" text NULL,
	"HEIGHT" float8 NULL,
	"SPEED" float8 NULL,
	"DISTANCE" float8 NULL,
	"SKY" text NULL,
	"PRECIPITATION" text NULL,
	"AOS" float8 NULL,
	"COST_REPAIRS" float8 NULL,
	"COST_OTHER" float8 NULL,
	"COST_REPAIRS_INFL_ADJ" float8 NULL,
	"COST_OTHER_INFL_ADJ" float8 NULL,
	"INGESTED_OTHER" int8 NULL,
	"INDICATED_DAMAGE" int8 NULL,
	"DAMAGE_LEVEL" text NULL,
	"STR_RAD" int8 NULL,
	"DAM_RAD" int8 NULL,
	"STR_WINDSHLD" int8 NULL,
	"DAM_WINDSHLD" int8 NULL,
	"STR_NOSE" int8 NULL,
	"DAM_NOSE" int8 NULL,
	"STR_ENG1" int8 NULL,
	"DAM_ENG1" int8 NULL,
	"ING_ENG1" int8 NULL,
	"STR_ENG2" int8 NULL,
	"DAM_ENG2" int8 NULL,
	"ING_ENG2" int8 NULL,
	"STR_ENG3" int8 NULL,
	"DAM_ENG3" int8 NULL,
	"ING_ENG3" int8 NULL,
	"STR_ENG4" int8 NULL,
	"DAM_ENG4" int8 NULL,
	"ING_ENG4" int8 NULL,
	"STR_PROP" int8 NULL,
	"DAM_PROP" int8 NULL,
	"STR_WING_ROT" int8 NULL,
	"DAM_WING_ROT" int8 NULL,
	"STR_FUSE" int8 NULL,
	"DAM_FUSE" int8 NULL,
	"STR_LG" int8 NULL,
	"DAM_LG" int8 NULL,
	"STR_TAIL" int8 NULL,
	"DAM_TAIL" int8 NULL,
	"STR_LGHTS" int8 NULL,
	"DAM_LGHTS" int8 NULL,
	"STR_OTHER" int8 NULL,
	"DAM_OTHER" int8 NULL,
	"OTHER_SPECIFY" text NULL,
	"EFFECT" text NULL,
	"EFFECT_OTHER" text NULL,
	"BIRD_BAND_NUMBER" float8 NULL,
	"SPECIES_ID" text NULL,
	"SPECIES" text NULL,
	"REMARKS" text NULL,
	"REMAINS_COLLECTED" int8 NULL,
	"REMAINS_SENT" int8 NULL,
	"WARNED" text NULL,
	"NUM_SEEN" text NULL,
	"NUM_STRUCK" text NULL,
	"SIZE" text NULL,
	"ENROUTE_STATE" text NULL,
	"NR_INJURIES" float8 NULL,
	"NR_FATALITIES" float8 NULL,
	"COMMENTS" text NULL,
	"REPORTED_NAME" text NULL,
	"REPORTED_TITLE" text NULL,
	"SOURCE" text NULL,
	"PERSON" text NULL,
	"LUPDATE" text NULL,
	"TRANSFER" int8 NULL
);
CREATE INDEX ix_raw_strike_reports_index ON raw.strike_reports USING btree (index);

-- raw.isd definition

-- Drop table

-- DROP TABLE raw.isd;

CREATE TABLE raw.isd (
	"index" int8 NULL,
	"USAF" text NULL,
	"WBAN" text NULL,
	"STATION NAME" text NULL,
	"CTRY" text NULL,
	"STATE" text NULL,
	"ICAO" text NULL,
	"LAT" float8 NULL,
	"LON" float8 NULL,
	"ELEV(M)" float8 NULL,
	"BEGIN" timestamp NULL,
	"END" timestamp NULL,
	meteostation_id text NULL
);


CREATE TABLE raw.service (
 id SERIAL,
 loaded_at timestamp DEFAULT now(),
 filename VARCHAR(100) NOT NULL
);

CREATE TABLE raw.airport_meteostation_link (
	airport_id text NULL,
	air_lat float8 NULL,
	air_lon float8 NULL,
	meteostation_id text NULL,
	station_lat float8 NULL,
	station_lon float8 NULL,
	distance float8 NULL,
	rn int8 NULL
);

create schema ods;