# IMDB Data Ingestion Pipeline

Fast pipeline that downloads IMDB datasets and loads them into PostgreSQL. Processes millions of records in seconds with ETag caching so it only downloads when data actually changes. About as close to real-time as you can get with IMDB's daily updates.

## Setup

You need PostgreSQL and [Task](https://taskfile.dev/) installed.

```bash
# Copy the config template and set your database password
cp .env.sample .env
# Edit .env and update PGPASSWORD

# Set up the database
task setup

# Download and load data
task ingest

# Check what happened
task status
```

## Commands

- `task setup` - Create database tables
- `task ingest` - Download and load data (skips if unchanged)
- `task status` - Show database status and sizes
- `task clean` - Clear cache to force re-download
- `task backup` - Backup the database
- `task shell` - Open PostgreSQL shell

## What it does

Downloads TSV files from [IMDB's Non-Commercial Datasets](https://developer.imdb.com/non-commercial-datasets/) and loads them into PostgreSQL tables. No fancy transformations, just the raw data exactly as IMDB provides it.

The ETag caching means once you've downloaded a dataset, it won't re-download unless IMDB actually updates their files.

## Data format

Tables match the IMDB format. For example, `name.basics`:

```sql
CREATE TABLE name.basics (
    nconst text PRIMARY KEY,
    primaryName text,
    birthYear integer,
    deathYear integer,
    primaryProfession text,  -- "actor,producer,director"
    knownForTitles text      -- "tt1234567,tt7654321"
);
```

Text fields with comma-separated values are stored as-is. You'll need to split them in your queries if you want individual values.

## License

[MIT](LICENSE)
