-- PostgreSQL functions for dataset processing and statistics

-- =============================================================================
-- BULK LOADING FUNCTIONS
-- =============================================================================

-- Function for bulk loading with constraint optimisation
CREATE OR REPLACE FUNCTION bulk_load (
    schema_name text,
    table_name text,
    url text,
    etag_file text
) RETURNS text AS $$
DECLARE
    full_table_name text;
    row_count bigint;
BEGIN
    full_table_name := schema_name || '.' || table_name;

    -- Drop constraints for maximum speed
    EXECUTE format(
        'ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s_pkey',
        full_table_name, table_name
    );

    -- Truncate table
    EXECUTE format('TRUNCATE %s', full_table_name);

    -- Ultra-fast COPY with ETag saving
    EXECUTE format(
        'COPY %s FROM PROGRAM ''curl --etag-save "%s" %s | gunzip -c'' ' ||
        'WITH DELIMITER E''\t'' QUOTE E''\b'' CSV HEADER NULL ''\N''',
        full_table_name, etag_file, url
    );

    row_count := get_row_count(schema_name, table_name);

    -- Restore primary key constraint
    EXECUTE format(
        'ALTER TABLE %s ADD CONSTRAINT %s_pkey PRIMARY KEY (nconst)',
        full_table_name, table_name
    );

    -- Return success message
    RETURN 'SUCCESS: Loaded ' || row_count || ' records';
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- STATUS AND MONITORING FUNCTIONS
-- =============================================================================

-- Function to get database status information
CREATE OR REPLACE FUNCTION get_database_status (
    table_name text
) RETURNS TABLE (status_line text) AS $$
BEGIN
    RETURN QUERY
    SELECT
        '   ' || table_name || ' (' ||
        get_row_count('name', table_name) || ' rows)' AS status_line;
END;
$$ LANGUAGE plpgsql;

-- Function to get current row count
CREATE OR REPLACE FUNCTION get_row_count (
    schema_name text,
    table_name text
) RETURNS bigint AS $$
DECLARE
    row_count bigint;
BEGIN
    EXECUTE format(
        'SELECT COUNT(*) FROM %I.%I',
        schema_name, table_name
    ) INTO row_count;
    RETURN row_count;
END;
$$ LANGUAGE plpgsql;

-- Function to get database size (formatted)
DROP FUNCTION IF EXISTS get_database_size();
CREATE OR REPLACE FUNCTION get_database_size()
RETURNS TEXT AS $$
BEGIN
    RETURN 'Database size: ' || pg_size_pretty(pg_database_size(current_database()));
END;
$$ LANGUAGE plpgsql;

-- Function to get table sizes (formatted)
DROP FUNCTION IF EXISTS get_table_sizes();
CREATE OR REPLACE FUNCTION get_table_sizes()
RETURNS TEXT AS $$
DECLARE
    result TEXT := 'Table sizes:';
    rec RECORD;
BEGIN
    FOR rec IN
        SELECT
            t.schemaname,
            t.tablename,
            pg_size_pretty(pg_total_relation_size(t.schemaname||'.'||t.tablename)) AS size
        FROM pg_tables t
        WHERE t.schemaname NOT IN ('information_schema', 'pg_catalog')
        ORDER BY pg_total_relation_size(t.schemaname||'.'||t.tablename) DESC
    LOOP
        result := result || E'\n  ' || rec.schemaname || '.' || rec.tablename || ': ' || rec.size;
    END LOOP;
    
    RETURN result;
END;
$$ LANGUAGE plpgsql;
