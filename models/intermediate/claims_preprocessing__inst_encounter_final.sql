-------------------------------------------------------------------------------
-- Author       Thu Xuan Vu
-- Created      June 2022
-- Purpose      Bring together encounters for merged and nonmerged institutional claims.
-------------------------------------------------------------------------------
-- Modification History
--
-------------------------------------------------------------------------------
{{ config(
    tags=["medical_claim"]
    ,enabled=var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))
) }}

select
   {{ cast_string_or_varchar('encounter_id') }} as encounter_id
   ,{{ cast_string_or_varchar('patient_id') }} as patient_id
   ,{{ cast_string_or_varchar('encounter_type') }} as encounter_type
   ,cast(encounter_start_date as date) as encounter_start_date
   ,cast(encounter_end_date as date) as encounter_end_date
   ,cast(paid_amount as numeric(38,2)) as paid_amount
   ,cast(charge_amount as numeric(38,2)) as charge_amount
from {{ ref('claims_preprocessing__inst_encounter_merge')}}

union all

select
   {{ cast_string_or_varchar('encounter_id') }} as encounter_id
   ,{{ cast_string_or_varchar('patient_id') }} as patient_id
   ,{{ cast_string_or_varchar('encounter_type') }} as encounter_type
   ,cast(encounter_start_date as date) as encounter_start_date
   ,cast(encounter_end_date as date) as encounter_end_date
   ,cast(paid_amount as numeric(38,2)) as paid_amount
   ,cast(charge_amount as numeric(38,2)) as charge_amount
from {{ ref('claims_preprocessing__inst_encounter_nonmerge')}}