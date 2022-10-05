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
) }}

select
   cast(encounter_id as varchar) as encounter_id
   ,cast(patient_id as varchar) as patient_id
   ,cast(encounter_type as varchar) as encounter_type
   ,cast(encounter_start_date as date) as encounter_start_date
   ,cast(encounter_end_date as date) as encounter_end_date
   ,cast(paid_amount as numeric(38,2)) as paid_amount
   ,cast(charge_amount as numeric(38,2)) as charge_amount
from {{ ref('inst_encounter_merge')}}

union all

select
   cast(encounter_id as varchar) as encounter_id
   ,cast(patient_id as varchar) as patient_id
   ,cast(encounter_type as varchar) as encounter_type
   ,cast(encounter_start_date as date) as encounter_start_date
   ,cast(encounter_end_date as date) as encounter_end_date
   ,cast(paid_amount as numeric(38,2)) as paid_amount
   ,cast(charge_amount as numeric(38,2)) as charge_amount
from {{ ref('inst_encounter_nonmerge')}}