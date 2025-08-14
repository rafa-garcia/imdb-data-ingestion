#!/bin/bash

AVAILABLE_DATASETS=("name_basics" "title_basics" "title_akas" "title_crew" "title_episode" "title_principals" "title_ratings")

get_all_datasets() {
    printf "%s" "${AVAILABLE_DATASETS[*]}"
}

get_dataset_config() {
    local dataset="$1"
    case "$dataset" in
        "name_basics")
            printf "%s %s %s %s" "$NAME_BASICS_URL" "$NAME_BASICS_TABLE" "$NAME_BASICS_SCHEMA" "$NAME_BASICS_ETAG_FILE"
            ;;
        "title_basics")
            printf "%s %s %s %s" "$TITLE_BASICS_URL" "$TITLE_BASICS_TABLE" "$TITLE_BASICS_SCHEMA" "$TITLE_BASICS_ETAG_FILE"
            ;;
        "title_akas")
            printf "%s %s %s %s" "$TITLE_AKAS_URL" "$TITLE_AKAS_TABLE" "$TITLE_AKAS_SCHEMA" "$TITLE_AKAS_ETAG_FILE"
            ;;
        "title_crew")
            printf "%s %s %s %s" "$TITLE_CREW_URL" "$TITLE_CREW_TABLE" "$TITLE_CREW_SCHEMA" "$TITLE_CREW_ETAG_FILE"
            ;;
        "title_episode")
            printf "%s %s %s %s" "$TITLE_EPISODE_URL" "$TITLE_EPISODE_TABLE" "$TITLE_EPISODE_SCHEMA" "$TITLE_EPISODE_ETAG_FILE"
            ;;
        "title_principals")
            printf "%s %s %s %s" "$TITLE_PRINCIPALS_URL" "$TITLE_PRINCIPALS_TABLE" "$TITLE_PRINCIPALS_SCHEMA" "$TITLE_PRINCIPALS_ETAG_FILE"
            ;;
        "title_ratings")
            printf "%s %s %s %s" "$TITLE_RATINGS_URL" "$TITLE_RATINGS_TABLE" "$TITLE_RATINGS_SCHEMA" "$TITLE_RATINGS_ETAG_FILE"
            ;;
        *)
            printf "Error: Unknown dataset '%s'\n" "$dataset" >&2
            printf "Available datasets: %s\n" "${AVAILABLE_DATASETS[*]}" >&2
            exit 1
            ;;
    esac
}

ingest_single_dataset() {
    local dataset_name="$1"
    local url table schema etag_file_name
    read -r url table schema etag_file_name <<< "$(get_dataset_config "$dataset_name")"

    mkdir -p "$CACHE_DIR"
    local etag_file="$CACHE_DIR/$etag_file_name"
    local start_time
    start_time=$(date +%s)

    # Pipeline display and processing
    show_streaming_progress "$dataset_name" "$url"
    show_database_prep "$dataset_name"
    show_ingestion_with_progress "$dataset_name" "$schema" "$table" "$url" "$etag_file"

    # Show completion
    local final_count
    final_count=$(psql -t -A -c "SELECT get_row_count('$schema', '$table');")
    local total_time=$(($(date +%s) - start_time))
    show_final_success "$dataset_name" "$final_count" "$total_time"
}

show_streaming_progress() {
  local dataset="$1"
  local url="$2"
  printf "==> \e[1m%s\e[0m\n" "Streaming $url"
}

show_database_prep() {
  local dataset="$1"
  printf "==> \e[1m%s\e[0m\n" "Preparing PostgreSQL for $dataset data"
}

show_ingestion_with_progress() {
  local dataset="$1" schema="$2" table="$3" url="$4" etag_file="$5"

  printf "==> \e[1m%s\e[0m\n" "Ingesting $dataset data into PostgreSQL"

  # Start database operation in background
  psql -c "SELECT bulk_load('$schema', '$table', '$url', '/$etag_file');" > /dev/null &
  local psql_pid=$!

  # Wait for completion
  wait $psql_pid
  local exit_code=$?

  # Create timestamp file on successful completion
  if [ $exit_code -eq 0 ]; then
    local timestamp_file="${etag_file%.etag}.timestamp"
    date -u +"%Y-%m-%d %H:%M:%S UTC" > "$timestamp_file"
  fi
  printf "==> \e[1m%s\e[0m\n" "Finalising data import and updating statistics"
}

show_final_success() {
  local dataset="$1"
  local final_count="$2"
  local total_time="$3"
  printf "%s dataset successfully ingested (%s records in %ss)\n" "$dataset" "$final_count" "$total_time"
}

show_cache() {
    for dataset in $(get_all_datasets); do
        local url table schema etag_file_name
        read -r url table schema etag_file_name <<< "$(get_dataset_config "$dataset")"
        local etag_file="$CACHE_DIR/$etag_file_name"

        if [ -f "$etag_file" ]; then
            local etag
            etag=$(cat "$etag_file")
            local timestamp_file="${etag_file%.etag}.timestamp"
            if [ -f "$timestamp_file" ]; then
                local timestamp
                timestamp=$(cat "$timestamp_file")
                printf "  %s: %s (downloaded: %s)\n" "$dataset" "$etag" "$timestamp"
            else
                printf "  %s: %s (download date unknown)\n" "$dataset" "$etag"
            fi
        else
            printf "  %s: No cache\n" "$dataset"
        fi
    done
}

ingest_datasets() {
    if [ $# -eq 0 ]; then
        # No arguments: ingest all datasets
        for dataset in $(get_all_datasets); do
            ingest_single_dataset "$dataset"
        done
    else
        # Arguments provided: ingest specific datasets
        for dataset in "$@"; do
            ingest_single_dataset "$dataset"
        done
    fi
}

check_cache() {
    local needs_update=false

    for dataset in $(get_all_datasets); do
        local url table schema etag_file_name
        read -r url table schema etag_file_name <<< "$(get_dataset_config "$dataset")"
        local etag_file="$CACHE_DIR/$etag_file_name"

        if [ -f "$etag_file" ]; then
            local http_code
            http_code=$(curl -s -w "%{http_code}" -o /dev/null --head --etag-compare "$etag_file" "$url")
            if [ "$http_code" = "304" ]; then
                printf "✓ %s is up to date (ETag unchanged)\n" "$dataset"
            else
                printf "→ %s has changed, needs update\n" "$dataset"
                needs_update=true
            fi
        else
            printf "→ %s has no cache, needs initial download\n" "$dataset"
            needs_update=true
        fi
    done

    [ "$needs_update" = "false" ] && { printf "✓ All datasets are up to date, skipping ingestion\n"; return 0; } || return 1
}

show_stats() {
    for dataset in $(get_all_datasets); do
        local url table schema etag_file_name
        read -r url table schema etag_file_name <<< "$(get_dataset_config "$dataset")"

        local count
        count=$(psql -t -A -c "SELECT get_row_count('$schema', '$table');")
        printf "Record count for %s: %s\n" "$dataset" "$count"

        [ "$count" -lt 1000000 ] && printf "Warning: %s record count seems low\n" "$dataset"
    done
}

check_setup() {
    local needs_setup=false

    # Check if tables exist
    local tables_missing=false
    for dataset in $(get_all_datasets); do
        local url table schema etag_file_name
        read -r url table schema etag_file_name <<< "$(get_dataset_config "$dataset")"
        if ! psql -t -c "SELECT to_regclass('$schema.$table');" | grep -q "$table"; then
            tables_missing=true
            break
        fi
    done
    
    if [ "$tables_missing" = "true" ]; then
        printf "→ Database tables need to be created\n"
        needs_setup=true
    else
        printf "✓ Database tables exist\n"
    fi

    # Check if functions exist
    if ! psql -t -c "SELECT count(*) FROM pg_proc WHERE proname='bulk_load';" | grep -q "1"; then
        printf "→ Database functions need to be installed\n"
        needs_setup=true
    else
        printf "✓ Database functions installed\n"
    fi

    # Check if scripts are executable
    if ! test -x "scripts/ingest.sh"; then
        printf "→ Script permissions need to be set\n"
        needs_setup=true
    else
        printf "✓ Script permissions set\n"
    fi

    if [ "$needs_setup" = "false" ]; then
        printf "✓ Database environment is already set up\n"
        return 1  # no setup needed
    else
        return 0  # setup needed
    fi
}
