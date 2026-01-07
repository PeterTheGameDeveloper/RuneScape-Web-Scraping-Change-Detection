/*
    Test: Ensure changes have been ingested recently
    Catches pipeline failures or stale data
*/

SELECT
    MAX(change_timestamp) AS latest_change,
    current_timestamp AS check_time,
    DATEDIFF('hour', MAX(change_timestamp), current_timestamp) AS hours_since_latest
FROM {{ ref('stg_wiki_changes') }}
HAVING DATEDIFF('hour', MAX(change_timestamp), current_timestamp) > {{ var('lookback_hours', 24) }}
