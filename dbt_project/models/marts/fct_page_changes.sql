{{
    config(
        materialized='table'
    )
}}

/*
    Fact table for page changes - deduplicated and enriched
*/

WITH staged AS (
    SELECT * FROM {{ ref('stg_wiki_changes') }}
),

-- Deduplicate by change_id (should already be unique, but safety first)
deduplicated AS (
    SELECT DISTINCT ON (change_id)
        *
    FROM staged
    ORDER BY change_id, ingested_at DESC
),

enriched AS (
    SELECT
        -- Grain: one row per change
        change_id,
        revid,
        old_revid,
        rcid,
        pageid,
        title,
        namespace,
        
        -- Timestamps
        change_timestamp,
        change_date,
        change_hour,
        
        -- Editor info
        editor_name,
        is_bot_edit,
        
        -- Edit details
        edit_comment,
        is_minor_edit,
        is_new_page,
        edit_type,
        
        -- Metrics
        old_length_bytes,
        new_length_bytes,
        size_diff_bytes,
        ABS(size_diff_bytes) AS absolute_size_change,
        
        -- Edit categorization
        CASE 
            WHEN ABS(size_diff_bytes) < 50 THEN 'tiny'
            WHEN ABS(size_diff_bytes) < 500 THEN 'small'
            WHEN ABS(size_diff_bytes) < 2000 THEN 'medium'
            ELSE 'large'
        END AS edit_size_category,
        
        -- Metadata
        ingested_at,
        run_id
        
    FROM deduplicated
)

SELECT * FROM enriched
