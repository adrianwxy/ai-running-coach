# Personal Running Performance Analysis


## Overview
This project tracks and analyzes my personal running journey as a recreational runner, from beginner level to a sub-2-hour half-marathoner.

This project is designed as a longitudinal performance log that contains training volume, intensity, effort, physiological signals, gear and weather context overtime. 

The primary goal is demonstrates end-to-end data engineering, data modeling and analytics thinking through the lens of distance running performance analysis, using real-world, evolving dataset.

Note: Personal data is exlcuded for privacy reasons.


## Tech Stack
- Database: SQLite with normalized schema design
- Data Sources:
  - Garmin Connect API
  - Strava API
  - Weather API (Open-Meteo)
  - Manuaal Logging (subjective metrics)
- Analysis: Python


## Database Schema
It's a SQLite database designed around training sessions, splits, gear, location and weather conditions. 

### Core Tables
- **main_log** - Primary activity records
  - Running metrics: distance, duration, pace, heart rate, power, cadence.
  - Subjective tracking: effort level (1-10), feel score (1-5).
  - Gear tracking: running shoe rotation via foreign key.
  - API integration: unique activity id from Strava and Garmin.
- **split_log** - Kilometer split performance data
  - Enables pacing strategy analysis
  - Track elevation changes and physiological response per KM segment.
- **weather_log** - Weather conditions during activity
  - Temperature, wind speed, humidity.
  - Hourly weather condition aligned to the actual run window.
- **shoe** - Gear tracking
  - Tracks mileage per shoe for rotation and retirement scheduling.
  - Carbon plate categorization for performance analysis.


### Design Decisions
- Composite primary keys support multiple runs per day.
- Check constraints enforce valid ranges for subjective metrics.
- Foreign key constraints maintain referential integrity.
- Default value to minimize redundant manual data entry while maintaining data quality.
- Created and updated timestamps enable audit trail and data versioning.
- Future extensibility (injury log, races, interactive dashboard).


## Core Questions
- How does consistent training translate into measurable performance improvement?
- How do effort, perceived feel, and physiology evolve over months of training?
- How do external factors (terrain, surface, shoes, weather) interact with performance?
- What weekly mileage correlates with good performance and endurance?


## Pipeline & Workflow
1. Ingest running activity metadata (manual or API-based)
2. Normalize and store runs in a relational schema
3. Fetch and align hourly weather data to run timestamps
4. Generate derived metrics (pace, elevation-adjusted effort, trends)
5. Perform exploratory and longitudinal analysis via notebooks


