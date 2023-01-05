-------------------------------------------------------------------------------
-- Author       Thu Xuan Vu
-- Created      June 2022
-- Purpose      Use recursion to umbrella claim pairs under one id.
-- Notes        
-------------------------------------------------------------------------------
-- Modification History
--
-------------------------------------------------------------------------------
{{ config(
    tags=["medical_claim"]
    ,enabled=var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))
) }}

with recursive stage as (
    select
        patient_id
        ,claim_id_a as group_claim_id
        , claim_id_a as claim_id
        , 1 as depth
    from {{ ref('claims_preprocessing__inst_merge_final')}}
    where claim_id_b is null

union all
  
    select
        a.patient_id
        ,s.group_claim_id
        ,a.claim_id_b as claim_id
        ,s.depth + 1 as depth
    from {{ ref('claims_preprocessing__inst_merge_final')}} a
    inner join stage s
        on a.claim_id_a = s.claim_id
  )

select * from stage
where claim_id is not null
  
