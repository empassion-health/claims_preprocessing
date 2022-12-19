-------------------------------------------------------------------------------
-- Author       Thu Xuan Vu
-- Created      June 2022
-- Purpose      Populate diagnosis and present on admission for a patient using
--                the claim sequence as diagnosis rank. 
-------------------------------------------------------------------------------
-- Modification History
-- 11/01/2022  Thu Xuan Vu
--      Removed case statement to normalize dx code type.  Should be done at mapping.
-------------------------------------------------------------------------------
{{ config(enabled=var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))) }}


with condition_code as(
  select
    encounter_id
    ,claim_id
    ,patient_id
    ,claim_start_date as condition_date
    ,diagnosis_code_type as code_type
    ,code
    ,cast(replace(lower(diagnosis_rank),'diagnosis_code_','') as int) as diagnosis_rank
    ,data_source
  from {{ ref('claims_preprocessing__encounter_claim_line_stage')}}
  unpivot(
    code for diagnosis_rank in (diagnosis_code_1
                                ,diagnosis_code_2
                                ,diagnosis_code_3
                                ,diagnosis_code_4
                                ,diagnosis_code_5
                                ,diagnosis_code_6
                                ,diagnosis_code_7
                                ,diagnosis_code_8
                                ,diagnosis_code_9
                                ,diagnosis_code_10
                                ,diagnosis_code_11
                                ,diagnosis_code_12
                                ,diagnosis_code_13
                                ,diagnosis_code_14
                                ,diagnosis_code_15
                                ,diagnosis_code_16
                                ,diagnosis_code_17
                                ,diagnosis_code_18
                                ,diagnosis_code_19
                                ,diagnosis_code_20
                                ,diagnosis_code_21
                                ,diagnosis_code_22
                                ,diagnosis_code_23
                                ,diagnosis_code_24
                                ,diagnosis_code_25)
            )pdx
)
, condition_poa as(
  select 
    claim_id
    ,present_on_admit_code
    ,cast(replace(lower(diagnosis_rank),'diagnosis_poa_','') as int) as diagnosis_rank
  from {{ ref('claims_preprocessing__encounter_claim_line_stage')}}
  unpivot(
    present_on_admit_code for diagnosis_rank in (diagnosis_poa_1
                                            ,diagnosis_poa_2
                                            ,diagnosis_poa_3
                                            ,diagnosis_poa_4
                                            ,diagnosis_poa_5
                                            ,diagnosis_poa_6
                                            ,diagnosis_poa_7
                                            ,diagnosis_poa_8
                                            ,diagnosis_poa_9
                                            ,diagnosis_poa_10
                                            ,diagnosis_poa_11
                                            ,diagnosis_poa_12
                                            ,diagnosis_poa_13
                                            ,diagnosis_poa_14
                                            ,diagnosis_poa_15
                                            ,diagnosis_poa_16
                                            ,diagnosis_poa_17
                                            ,diagnosis_poa_18
                                            ,diagnosis_poa_19
                                            ,diagnosis_poa_20
                                            ,diagnosis_poa_21
                                            ,diagnosis_poa_22
                                            ,diagnosis_poa_23
                                            ,diagnosis_poa_24
                                            ,diagnosis_poa_25)
            )ppoa
)
select distinct
  {{ cast_string_or_varchar('c.encounter_id') }} as encounter_id
  ,{{ cast_string_or_varchar('c.patient_id') }} as patient_id
  ,cast(c.condition_date as date) as condition_date
  ,cast('discharge diagnosis' as varchar) as condition_type
  ,{{ cast_string_or_varchar('c.code_type') }} as code_type
  ,cast(replace(c.code,'.','') as varchar) as code
  ,{{ cast_string_or_varchar('dx.short_description') }} as description
  ,cast(c.diagnosis_rank as int) as diagnosis_rank
  ,{{ cast_string_or_varchar('p.present_on_admit_code') }} as present_on_admit_code
  ,{{ cast_string_or_varchar('poa.present_on_admit_description') }} as present_on_admit_description
  ,{{ cast_string_or_varchar('data_source') }} as data_source
from condition_code c
left join condition_poa p
  ON c.claim_id = p.claim_id
  AND c.diagnosis_rank = p.diagnosis_rank
left join {{ ref('terminology__icd_10_cm')}} dx
  on c.code = icd_10_cm
  and c.code_type in ('icd-10-cm')
left join {{ ref ('terminology__present_on_admission')}} poa
  on p.present_on_admit_code = poa.present_on_admit_code
where code <> ''