/*
    Test: Ensure no change timestamps are in the future
    This catches data quality issues from incorrect timestamp parsing
*/

SELECT
    change_id,
    change_timestamp,
    current_timestamp AS check_time
FROM {{ ref('stg_wiki_changes') }}
WHERE change_timestamp > current_timestamp + INTERVAL '1 hour'
