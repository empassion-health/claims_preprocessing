---------------------------------------------------------------------------------------------------------
-- Author       Thu Xuan Vu
-- Created      May 2022
-- Purpose      Union encounter mappings and using header value for merging process to remove duplicates
-- Notes        Duplicates may occur at the line level.  Using the header value to determind claim type.
---------------------------------------------------------------------------------------------------------
-- Modification History
--
---------------------------------------------------------------------------------------------------------
{{ config(
    tags=["medical_claim"]
) }}

with claim_header as(
    select 
        claim_id
        ,min(claim_line_number) as claim_line_number
    from {{ var('medical_claim')}}
    group by
        claim_id
)
, encounter_type_union_stage as(
  select
    cast(claim_id as varchar) as claim_id
    ,cast(claim_line_number as int) as claim_line_number
    ,cast(claim_type as varchar) as claim_type
    ,cast(patient_id as varchar) as patient_id
    ,cast(encounter_type as varchar) as encounter_type
    ,cast(claim_start_date as date) as claim_start_date
    ,cast(claim_end_date as date) as claim_end_date
    ,cast(discharge_disposition_code as varchar) as discharge_disposition_code
    ,cast(billing_npi as varchar) as billing_npi
    ,cast(facility_npi as varchar) as facility_npi
    ,cast(paid_amount as numeric(38,2)) as paid_amount
    ,cast(charge_amount as numeric(38,2)) as charge_amount
    ,cast(bill_type_code as varchar) as bill_type_code
    ,cast(revenue_center_code as varchar) as revenue_center_code
    ,cast(place_of_service_code as varchar) as place_of_service_code
  from {{ ref('encounter_type_mapping')}}

  union all

  select
    cast(med.claim_id as varchar) as claim_id
    ,cast(med.claim_line_number as int) as claim_line_number
    ,cast(med.claim_type as varchar) as claim_type
    ,cast(med.patient_id as varchar) as patient_id
    ,cast(ainp.encounter_type as varchar) as encounter_type
    ,cast(med.claim_start_date as date) as claim_start_date
    ,cast(med.claim_end_date as date) as claim_end_date
    ,cast(med.discharge_disposition_code as varchar) as discharge_disposition_code
    ,cast(med.billing_npi as varchar) as billing_npi
    ,cast(med.facility_npi as varchar) as facility_npi
    ,cast(med.paid_amount as numeric(38,2)) as paid_amount
    ,cast(med.charge_amount as numeric(38,2)) as charge_amount
    ,cast(ainp.bill_type_code as varchar) as bill_type_code
    ,cast(ainp.revenue_center_code as varchar) as revenue_center_code
    ,cast(ainp.place_of_service_code as varchar) as place_of_service_code
  from {{ var('medical_claim')}} med
  inner join claim_header head
    on med.claim_id = head.claim_id
    and med.claim_line_number = head.claim_line_number
  inner join {{ ref('acute_inpatient_encounter')}} ainp
    on head.claim_id = ainp.claim_id

  union all

  select
    cast(med.claim_id as varchar) as claim_id
    ,cast(med.claim_line_number as int) as claim_line_number
    ,cast(med.claim_type as varchar) as claim_type
    ,cast(med.patient_id as varchar) as patient_id
    ,cast(ed.encounter_type as varchar) as encounter_type
    ,cast(med.claim_start_date as date) as claim_start_date
    ,cast(med.claim_end_date as date) as claim_end_date
    ,cast(med.discharge_disposition_code as varchar) as discharge_disposition_code
    ,cast(med.billing_npi as varchar) as billing_npi
    ,cast(med.facility_npi as varchar) as facility_npi
    ,cast(med.paid_amount as numeric(38,2)) as paid_amount
    ,cast(med.charge_amount as numeric(38,2)) as charge_amount
    ,cast(med.bill_type_code as varchar) as bill_type_code
    ,cast(med.revenue_center_code as varchar) as revenue_center_code
    ,cast(med.place_of_service_code as varchar) as place_of_service_code
  from {{ var('medical_claim')}} med
  inner join claim_header head
    on med.claim_id = head.claim_id
    and med.claim_line_number = head.claim_line_number
  inner join {{ ref('emergency_department_encounter')}} ed
    on head.claim_id = ed.claim_id
  left join {{ ref('acute_inpatient_encounter')}} ainp
    on ed.claim_id = ainp.claim_id
  where ainp.claim_id is null  
)

select 
  cast(claim_id as varchar) as claim_id
  ,cast(claim_line_number as int) as claim_line_number
  ,cast(lower(claim_type) as varchar) as claim_type
  ,cast(patient_id as varchar) as patient_id
  ,cast(encounter_type as varchar) as encounter_type
  ,cast(claim_start_date as date) as claim_start_date
  ,cast(claim_end_date as date) as claim_end_date
  ,cast(discharge_disposition_code as varchar) as discharge_disposition_code
  ,cast(billing_npi as varchar) as billing_npi
  ,cast(facility_npi as varchar) as facility_npi
  ,cast(paid_amount as numeric(38,2)) as paid_amount
  ,cast(charge_amount as numeric(38,2)) as charge_amount
  ,cast(bill_type_code as varchar) as bill_type_code
  ,cast(revenue_center_code as varchar) as revenue_center_code
  ,cast(place_of_service_code as varchar) as place_of_service_code
  ,cast(row_number() over (partition by patient_id, encounter_type, claim_type order by claim_start_date, claim_end_date) as int) as row_sequence
from encounter_type_union_stage