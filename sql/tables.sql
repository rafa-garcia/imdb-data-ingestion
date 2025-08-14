-- IMDB Data Ingestion Schema
-- Raw data storage tables (no transformation/normalisation)

-- Create base schemas
CREATE SCHEMA IF NOT EXISTS "name";

-- Performance settings for bulk operations
SET maintenance_work_mem = '1GB';
SET work_mem = '256MB';

-- =============================================================================
-- NAME TABLES
-- =============================================================================
-- Drop name tables in correct order
DROP TABLE IF EXISTS "name"."basics" CASCADE;

-- Main table for name basics
CREATE TABLE "name"."basics" (
    "nconst" text PRIMARY KEY,
    "primaryName" text,
    "birthYear" integer,
    "deathYear" integer,
    "primaryProfession" text,
    "knownForTitles" text
);
