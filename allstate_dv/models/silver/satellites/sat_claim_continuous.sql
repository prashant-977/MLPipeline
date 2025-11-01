{{
    config(
        materialized='incremental',
        unique_key=['claim_hashkey', 'load_timestamp'],
        incremental_strategy='append'
    )
}}

WITH source_data AS (
    SELECT
        id as claim_id,
        cont1, cont2, cont3, cont4, cont5, cont6, cont7, cont8, cont9, cont10,
        cont11, cont12, cont13, cont14,
        loss,
        load_timestamp,
        record_source
    FROM {{ source('bronze', 'allstate_claims') }}
    {% if is_incremental() %}
        WHERE load_timestamp > (SELECT MAX(load_timestamp) FROM {{ this }})
    {% endif %}
)

SELECT
    {{ generate_hash_key(['claim_id']) }} as claim_hashkey,
    cont1, cont2, cont3, cont4, cont5, cont6, cont7, cont8, cont9, cont10,
    cont11, cont12, cont13, cont14,
    loss,
    load_timestamp,
    record_source,
    {{ generate_hash_key(['cont1', 'cont2', 'cont3', 'cont4', 'cont5', 'cont6', 'cont7', 'cont8', 'cont9', 'cont10', 'cont11', 'cont12', 'cont13', 'cont14', 'loss']) }} as hashdiff
FROM source_data