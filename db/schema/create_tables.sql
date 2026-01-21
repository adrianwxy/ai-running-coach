CREATE TABLE "main_log" (
	"log_id"	INTEGER,
	"date"	TEXT NOT NULL,
	"run_of_the_day"	INTEGER NOT NULL DEFAULT 1,
	"time"	TEXT,
	"latitude"	REAL,
	"longitude"	REAL,
	"city"	TEXT DEFAULT 'ABCD',
	"state"	TEXT DEFAULT 'EF',
	"distance_km"	REAL NOT NULL,
	"duration_seconds"	INTEGER NOT NULL,
	"active_cal"	REAL,
	"elevation_gain_meter"	INTEGER,
	"avg_power_w"	REAL,
	"avg_cadence_spm"	REAL,
	"avg_heart_rate_bpm"	INTEGER,
	"effort_level"	INTEGER DEFAULT 3 CHECK("effort_level" BETWEEN 1 AND 10),
	"feel_score"	INTEGER DEFAULT 4 CHECK("feel_score" BETWEEN 1 AND 5),
	"run_type"	TEXT,
	"surface"	TEXT,
	"notes"	TEXT,
	"shoe_id"	INTEGER NOT NULL DEFAULT 1,
	"garmin_activity_id"	TEXT UNIQUE,
	"strava_activity_id"	TEXT UNIQUE,
	"created_at_utc"	TEXT DEFAULT (datetime('now')),
	"updated_at_utc"	TEXT DEFAULT (datetime('now')),
	UNIQUE("date","run_of_the_day"),
	PRIMARY KEY("log_id" AUTOINCREMENT),
	FOREIGN KEY("shoe_id") REFERENCES "shoes"("id")
);


CREATE TABLE "split_log" (
	"date"	TEXT NOT NULL,
	"run_of_the_day"	INTEGER NOT NULL,
	"split_index"	INTEGER NOT NULL,
	"distance_km"	REAL NOT NULL,
	"duration_seconds"	INTEGER NOT NULL,
	"is_partial_split"	INTEGER NOT NULL DEFAULT 0,
	"avg_heart_rate_bpm"	INTEGER,
	"max_heart_rate_bpm"	INTEGER,
	"ascent_meter"	REAL,
	"descent_meter"	REAL,
	"avg_power_w"	REAL,
	"avg_cadence_spm"	REAL,
	PRIMARY KEY("date","run_of_the_day","split_index"),
	UNIQUE("date","run_of_the_day","split_index"),
	FOREIGN KEY("date","run_of_the_day") REFERENCES "main_log"("date","run_of_the_day")
);


CREATE TABLE "shoes" (
	id INTEGER PRIMARY KEY,
	brand TEXT NOT NULL,
	full_name TEXT NOT NULL,
	retired INTEGER DEFAULT 0 CHECK (retired IN (0,1)), -- 0 active, 1 retired
	carbon_insert INTEGER DEFAULT 0 CHECK (carbon_insert IN (0,1))-- 0 N, 1 Y 
);


CREATE TABLE "weather_log" (
	"date"	TEXT NOT NULL,
	"time"	TEXT NOT NULL,
	"latitude"	REAL,
	"longitude"	REAL,
	"temp_celsius"	REAL,
	"humidity_pct"	REAL,
	"wind_speed_kph"	REAL,
	"weather_condition"	TEXT,
	"created_at_utc"	TEXT DEFAULT (datetime('now')),
	PRIMARY KEY("date","time")
);

