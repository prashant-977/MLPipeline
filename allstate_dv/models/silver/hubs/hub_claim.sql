{{
    config(
        materialized='incremental',
        unique_key='claim_hashkey',
        incremental_strategy='merge'
    )
}}

WITH source_data AS (
    SELECT DISTINCT
        id as claim_id,
        load_timestamp,
        record_source
    FROM {{ source('bronze', 'allstate_claims') }}
    {% if is_incremental() %}
        WHERE load_timestamp > (SELECT MAX(load_timestamp) FROM {{ this }})
    {% endif %}
)

SELECT
    {{ generate_hash_key(['claim_id']) }} as claim_hashkey,
    claim_id,
    load_timestamp,
    record_source
FROM source_data