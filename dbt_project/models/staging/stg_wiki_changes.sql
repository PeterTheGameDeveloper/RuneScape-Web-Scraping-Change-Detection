{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for wiki changes - cleans and standardizes raw data
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_wiki_changes') }}
),

cleaned AS (
    SELECT
        -- Identifiers
        change_id,
        revid,
        old_revid,
        rcid,
        pageid,
        
        -- Page info
        title,
        namespace,
        
        -- Change metadata
        timestamp AS change_timestamp,
        user_name AS editor_name,
        COALESCE(comment, '') AS edit_comment,
        
        -- Size metrics
        COALESCE(old_len, 0) AS old_length_bytes,
        COALESCE(new_len, 0) AS new_length_bytes,
        COALESCE(size_diff, new_len - old_len, 0) AS size_diff_bytes,
        
        -- Flags
        COALESCE(is_minor, FALSE) AS is_minor_edit,
        COALESCE(is_bot, FALSE) AS is_bot_edit,
        COALESCE(is_new, FALSE) AS is_new_page,
        
        -- Ingestion metadata
        ingested_at,
        run_id,
        
        -- Derived fields
        CASE 
            WHEN size_diff > 0 THEN 'addition'
            WHEN size_diff < 0 THEN 'removal'
            ELSE 'no_change'
        END AS edit_type,
        
        DATE_TRUNC('day', timestamp) AS change_date,
        DATE_TRUNC('hour', timestamp) AS change_hour

    FROM source
)

SELECT * FROM cleaned
