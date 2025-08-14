-- IMDB Data Ingestion Schema
-- Raw data storage tables (no transformation/normalisation)

-- Create base schemas
CREATE SCHEMA IF NOT EXISTS "name";
CREATE SCHEMA IF NOT EXISTS "title";

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

-- =============================================================================
-- TITLE TABLES
-- =============================================================================
-- Drop title tables in correct order
DROP TABLE IF EXISTS "title"."basics" CASCADE;
DROP TABLE IF EXISTS "title"."akas" CASCADE;

-- Main table for title basics
CREATE TABLE "title"."basics" (
    "tconst" text PRIMARY KEY,
    "titleType" text,
    "primaryTitle" text,
    "originalTitle" text,
    "isAdult" integer,
    "startYear" integer,
    "endYear" integer,
    "runtimeMinutes" integer,
    "genres" text
);

-- Main table for title akas
CREATE TABLE "title"."akas" (
    "titleId" text,
    "ordering" integer,
    "title" text,
    "region" text,
    "language" text,
    "types" text,
    "attributes" text,
    "isOriginalTitle" integer,
    PRIMARY KEY ("titleId", "ordering")
);
