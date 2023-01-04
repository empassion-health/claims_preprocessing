{{ config(enabled=var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))) }}

with encounter_type_union_stage as(
  select
      cast(claim_id as string) as claim_id
    , cast(claim_line_number as int) as claim_line_number
    , cast(claim_type as string) as claim_type
    , cast(patient_id as string) as patient_id
    , cast(encounter_type as string) as encounter_type
    , cast(claim_start_date as date) as claim_start_date
    , cast(claim_end_date as date) as claim_end_date
    , cast(discharge_disposition_code as string) as discharge_disposition_code
    , cast(billing_npi as string) as billing_npi
    , cast(facility_npi as string) as facility_npi
    , cast(paid_amount as numeric(38,2)) as paid_amount
    , cast(charge_amount as numeric(38,2)) as charge_amount
    , cast(bill_type_code as string) as bill_type_code
    , cast(revenue_center_code as string) as revenue_center_code
    , cast(place_of_service_code as string) as place_of_service_code
    , cast(row_number() over (partition by claim_id order by claim_line_number) as int) as row_number
  from {{ ref('claims_preprocessing__encounter_type_mapping')}}

  union all

  select
      cast(med.claim_id as string) as claim_id
    , cast(med.claim_line_number as int) as claim_line_number
    , cast(med.claim_type as string) as claim_type
    , cast(med.patient_id as string) as patient_id
    , cast(ainp.encounter_type as string) as encounter_type
    , cast(med.claim_start_date as date) as claim_start_date
    , cast(med.claim_end_date as date) as claim_end_date
    , cast(med.discharge_disposition_code as string) as discharge_disposition_code
    , cast(med.billing_npi as string) as billing_npi
    , cast(med.facility_npi as string) as facility_npi
    , cast(med.paid_amount as numeric(38,2)) as paid_amount
    , cast(med.charge_amount as numeric(38,2)) as charge_amount
    , cast(ainp.bill_type_code as string) as bill_type_code
    , cast(ainp.revenue_center_code as string) as revenue_center_code
    , cast(ainp.place_of_service_code as string) as place_of_service_code
    , cast(row_number() over (partition by med.claim_id order by med.claim_line_number) as int) as row_number
  from {{ var('medical_claim')}} med
  inner join {{ ref('claims_preprocessing__acute_inpatient_encounter')}} ainp
    on med.claim_id = ainp.claim_id    

  union all

  select
      cast(med.claim_id as string) as claim_id
    , cast(med.claim_line_number as int) as claim_line_number
    , cast(med.claim_type as string) as claim_type
    , cast(med.patient_id as string) as patient_id
    , cast(ed.encounter_type as string) as encounter_type
    , cast(med.claim_start_date as date) as claim_start_date
    , cast(med.claim_end_date as date) as claim_end_date
    , cast(med.discharge_disposition_code as string) as discharge_disposition_code
    , cast(med.billing_npi as string) as billing_npi
    , cast(med.facility_npi as string) as facility_npi
    , cast(med.paid_amount as numeric(38,2)) as paid_amount
    , cast(med.charge_amount as numeric(38,2)) as charge_amount
    , cast(med.bill_type_code as string) as bill_type_code
    , cast(med.revenue_center_code as string) as revenue_center_code
    , cast(med.place_of_service_code as string) as place_of_service_code
    , cast(row_number() over (partition by med.claim_id order by med.claim_line_number) as int) as row_number
  from {{ var('medical_claim')}} med
  inner join {{ ref('claims_preprocessing__emergency_department_encounter')}} ed
    on med.claim_id = ed.claim_id
  left join {{ ref('claims_preprocessing__acute_inpatient_encounter')}} ainp
    on ed.claim_id = ainp.claim_id
  where ainp.claim_id is null  
)

select 
    cast(claim_id as string) as claim_id
  , cast(claim_line_number as int) as claim_line_number
  , cast(lower(claim_type) as string) as claim_type
  , cast(patient_id as string) as patient_id
  , cast(encounter_type as string) as encounter_type
  , cast(claim_start_date as date) as claim_start_date
  , cast(claim_end_date as date) as claim_end_date
  , cast(discharge_disposition_code as string) as discharge_disposition_code
  , cast(billing_npi as string) as billing_npi
  , cast(facility_npi as string) as facility_npi
  , cast(paid_amount as numeric(38,2)) as paid_amount
  , cast(charge_amount as numeric(38,2)) as charge_amount
  , cast(bill_type_code as string) as bill_type_code
  , cast(revenue_center_code as string) as revenue_center_code
  , cast(place_of_service_code as string) as place_of_service_code
  , cast(row_number() over (partition by patient_id, encounter_type, claim_type order by claim_start_date, claim_end_date) as int) as row_sequence
from encounter_type_union_stage
where row_number = 1