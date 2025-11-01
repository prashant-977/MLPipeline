{{
    config(
        materialized='table'
    )
}}

WITH hub AS (
    SELECT 
        claim_hashkey,
        claim_id
    FROM {{ ref('hub_claim') }}
),

sat_cat AS (
    SELECT 
        claim_hashkey,
        cat1, cat2, cat3, cat4, cat5, cat6, cat7, cat8, cat9, cat10,
        cat11, cat12, cat13, cat14, cat15, cat16, cat17, cat18, cat19, cat20,
        cat21, cat22, cat23, cat24, cat25, cat26, cat27, cat28, cat29, cat30,
        cat31, cat32, cat33, cat34, cat35, cat36, cat37, cat38, cat39, cat40,
        cat41, cat42, cat43, cat44, cat45, cat46, cat47, cat48, cat49, cat50,
        cat51, cat52, cat53, cat54, cat55, cat56, cat57, cat58, cat59, cat60,
        cat61, cat62, cat63, cat64, cat65, cat66, cat67, cat68, cat69, cat70,
        cat71, cat72, cat73, cat74, cat75, cat76, cat77, cat78, cat79, cat80,
        cat81, cat82, cat83, cat84, cat85, cat86, cat87, cat88, cat89, cat90,
        cat91, cat92, cat93, cat94, cat95, cat96, cat97, cat98, cat99, cat100,
        cat101, cat102, cat103, cat104, cat105, cat106, cat107, cat108, cat109, cat110,
        cat111, cat112, cat113, cat114, cat115, cat116,
        ROW_NUMBER() OVER (PARTITION BY claim_hashkey ORDER BY load_timestamp DESC) as rn
    FROM {{ ref('sat_claim_categorical') }}
),

sat_cont AS (
    SELECT 
        claim_hashkey,
        cont1, cont2, cont3, cont4, cont5, cont6, cont7, cont8, cont9, cont10,
        cont11, cont12, cont13, cont14,
        loss,
        ROW_NUMBER() OVER (PARTITION BY claim_hashkey ORDER BY load_timestamp DESC) as rn
    FROM {{ ref('sat_claim_continuous') }}
)

SELECT 
    h.claim_id,
    -- Categorical features
    sc.cat1, sc.cat2, sc.cat3, sc.cat4, sc.cat5, sc.cat6, sc.cat7, sc.cat8, sc.cat9, sc.cat10,
    sc.cat11, sc.cat12, sc.cat13, sc.cat14, sc.cat15, sc.cat16, sc.cat17, sc.cat18, sc.cat19, sc.cat20,
    sc.cat21, sc.cat22, sc.cat23, sc.cat24, sc.cat25, sc.cat26, sc.cat27, sc.cat28, sc.cat29, sc.cat30,
    sc.cat31, sc.cat32, sc.cat33, sc.cat34, sc.cat35, sc.cat36, sc.cat37, sc.cat38, sc.cat39, sc.cat40,
    sc.cat41, sc.cat42, sc.cat43, sc.cat44, sc.cat45, sc.cat46, sc.cat47, sc.cat48, sc.cat49, sc.cat50,
    sc.cat51, sc.cat52, sc.cat53, sc.cat54, sc.cat55, sc.cat56, sc.cat57, sc.cat58, sc.cat59, sc.cat60,
    sc.cat61, sc.cat62, sc.cat63, sc.cat64, sc.cat65, sc.cat66, sc.cat67, sc.cat68, sc.cat69, sc.cat70,
    sc.cat71, sc.cat72, sc.cat73, sc.cat74, sc.cat75, sc.cat76, sc.cat77, sc.cat78, sc.cat79, sc.cat80,
    sc.cat81, sc.cat82, sc.cat83, sc.cat84, sc.cat85, sc.cat86, sc.cat87, sc.cat88, sc.cat89, sc.cat90,
    sc.cat91, sc.cat92, sc.cat93, sc.cat94, sc.cat95, sc.cat96, sc.cat97, sc.cat98, sc.cat99, sc.cat100,
    sc.cat101, sc.cat102, sc.cat103, sc.cat104, sc.cat105, sc.cat106, sc.cat107, sc.cat108, sc.cat109, sc.cat110,
    sc.cat111, sc.cat112, sc.cat113, sc.cat114, sc.cat115, sc.cat116,
    -- Continuous features
    sct.cont1, sct.cont2, sct.cont3, sct.cont4, sct.cont5, 
    sct.cont6, sct.cont7, sct.cont8, sct.cont9, sct.cont10,
    sct.cont11, sct.cont12, sct.cont13, sct.cont14,
    -- Target
    sct.loss
FROM hub h
LEFT JOIN sat_cat sc ON h.claim_hashkey = sc.claim_hashkey AND sc.rn = 1
LEFT JOIN sat_cont sct ON h.claim_hashkey = sct.claim_hashkey AND sct.rn = 1