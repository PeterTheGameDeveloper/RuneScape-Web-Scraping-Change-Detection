{{
    config(
        materialized='table'
    )
}}

/*
    Dimension table for wiki pages - aggregated page-level metrics
*/

WITH changes AS (
    SELECT * FROM {{ ref('fct_page_changes') }}
),

page_metrics AS (
    SELECT
        pageid,
        title,
        namespace,
        
        -- Activity metrics
        COUNT(*) AS total_edits,
        COUNT(DISTINCT editor_name) AS unique_editors,
        SUM(CASE WHEN is_new_page THEN 1 ELSE 0 END) AS times_created,
        
        -- Edit type breakdown
        SUM(CASE WHEN edit_type = 'addition' THEN 1 ELSE 0 END) AS addition_count,
        SUM(CASE WHEN edit_type = 'removal' THEN 1 ELSE 0 END) AS removal_count,
        SUM(CASE WHEN is_minor_edit THEN 1 ELSE 0 END) AS minor_edit_count,
        SUM(CASE WHEN is_bot_edit THEN 1 ELSE 0 END) AS bot_edit_count,
        
        -- Size metrics
        SUM(size_diff_bytes) AS net_size_change,
        SUM(CASE WHEN size_diff_bytes > 0 THEN size_diff_bytes ELSE 0 END) AS total_bytes_added,
        SUM(CASE WHEN size_diff_bytes < 0 THEN ABS(size_diff_bytes) ELSE 0 END) AS total_bytes_removed,
        MAX(new_length_bytes) AS max_page_size,
        
        -- Temporal metrics
        MIN(change_timestamp) AS first_edit_at,
        MAX(change_timestamp) AS last_edit_at,
        
        -- Latest state
        FIRST(new_length_bytes ORDER BY change_timestamp DESC) AS current_size_bytes,
        FIRST(revid ORDER BY change_timestamp DESC) AS current_revid

    FROM changes
    GROUP BY pageid, title, namespace
)

SELECT
    *,
    -- Calculated fields
    DATEDIFF('day', first_edit_at, last_edit_at) AS active_days_span,
    total_edits - minor_edit_count AS substantive_edits,
    total_edits - bot_edit_count AS human_edits,
    CASE 
        WHEN total_edits = 1 THEN 'single_edit'
        WHEN total_edits <= 5 THEN 'low_activity'
        WHEN total_edits <= 20 THEN 'moderate_activity'
        ELSE 'high_activity'
    END AS activity_level

FROM page_metrics
