# Local AI Running Coach & Longitudinal Performance System


## Overview
This project is a fully local, end-to-end date and AI system built to track, analyze, and coach my long-term progression as a distance runner - to achieve sub-3-hour marathon goal.

Rather than a single analysis or model, the system is designed as a **production-style data platform** that ingests real-world workout data, stores it in a normalized relational schema, and exposes it to both analytical workflows and an LLM-powered coaching layer.

The project emphasizes **data engineering, system design, and AI integration** over one-off metrics or dashboards.

> Note: Raw personal data is excluded from this repository for privacy reasons.

## System Capabilities
- Longitudinal tracking of training volume, intensity, effort, and physiology
- Orchestrated ingestion from multiple external APIs
- Relational data modeling with split-level granularity
- Local LLM-driven coaching using historical and contextual memory
- Privacy-first, cloud-independent architecture

## Architecture Overview
**Data Layer**
- Local SQLite database serves as the source of truth
- Normalized schema supports:
  - multiple runs per day
  - split-level performance analysis
  - gear rotation tracking
  - weather and location context

**Ingestion Layer**
- Modular ETL pipeline with table-specific extract / transform / load scripts
- Master orchestration layer coordinates multi-source ingestion
- External sources:
  - Garmin Connect
  - Strava
  - Open-Meteo (hourly weather)
- Credentials managed via environment variables (`.env`)

**AI & Memory Layer**
- Fully local LLM stack via Ollama (Gemma3:12B)
- ChromaDB vector store used for long-term coaching memory
- Chronological indexing with semantic retrieval:
  - top-3 relevant conversations within a 30-day sliding window
  - distance-based relevance filtering (< 1.0 threshold)
  - recency-aware re-ranking to reduce noise
- SQL-backed context injection enables the model to reason over cumulative training load and recent performance trends


## Tech Stack
- **Database**: SQLite
- **Languages**: Python
- **APIs**:
  - Garmin Connect
  - Strava
  - Open-Meteo
- **LLM Runtime**: Ollama (Gemma3:12B)
- **Vector Database**: ChromaDB
- **Analysis**: Pandas, Jupyter



## Database Schema
It's a SQLite database designed around training sessions, splits per session, gear, location and weather conditions. 

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
- Composite primary keys support multiple runs per day
- Check constraints enforce valid subjective score ranges
- Defaults reduce redundant manual input while preserving flexibility
- Foreign keys maintain referential integrity across logs
- Timestamps enable auditability and historical consistency
- Schema-first approach enables downstream analytics and AI usage


## Core Questions
- How does consistent training translate into measurable performance improvement?
- How do effort, perceived feel, and physiological signals evolve over time?
- What indicators signal overtraining, fatigue, or readiness to increase load?
- How do gear choices and environmental conditions interact with performance?
- How can historical context improve coaching recommendations?


## Pipeline & Workflow
1. Extract activity and metadata from external APIs
2. Normalize and validate data into relational schema
3. Enrich sessions with aligned hourly weather data
4. Generate derived metrics and training load indicators
5. Surface structured context to an LLM for coaching and planning
6. Perform exploratory and longitudinal analysis via notebooks


## Future Work
- Synthetic dataset for full public reproducibility
- Race and training block tagging
- Injury and recovery annotations
- Automated weekly / monthly summaries
- Visualization layer or dashboard


