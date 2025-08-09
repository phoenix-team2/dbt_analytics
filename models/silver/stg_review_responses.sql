with raw as(
    SELECT *
    FROM
    {{source('bronze','review_responses')}}

),

renamed as (
    SELECT 
    RESPONSE_ID AS response_id,
    REVIEW_ID AS review_id, 
    CAST(response_date AS DATE) AS response_date,
    TRIM(RESPONSE_TEXT) AS response_text,
    RESPONDER_NAME AS responder_name,
    INITCAP(TRIM(RESPONDER_TYPE)) AS responder_type,
    ROW_NUMBER() OVER 
    (PARTITION BY RESPONSE_ID ORDER BY _AIRBYTE_EXTRACTED_AT DESC, COALESCE(_AIRBYTE_GENERATION_ID, 0) DESC)
    AS rn
    FROM raw
)

SELECT
response_id,
review_id,
response_date,
response_text,
responder_name,
responder_type,
CURRENT_TIMESTAMP AS _loaded_at
FROM renamed
WHERE rn=1