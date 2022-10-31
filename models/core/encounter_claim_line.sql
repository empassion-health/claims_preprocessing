-------------------------------------------------------------------------------
-- Author       Thu Xuan Vu
-- Created      June 2022
-- Purpose      Populate medical claim line level detail.
-------------------------------------------------------------------------------
-- Modification History
--
-------------------------------------------------------------------------------
{{ config(
    tags=["medical_claim","core"]
) }}

select 
  cast(claim_id as varchar) as claim_id
  ,cast(claim_line_number as int) as claim_line_number
  ,cast(claim_type as varchar) as claim_type
  ,cast(encounter_id as varchar) as encounter_id
  ,cast(patient_id as varchar) as patient_id
  ,cast(claim_start_date as date) as claim_start_date
  ,cast(claim_end_date as date) as claim_end_date
  ,cast(claim_line_start_date as date) as claim_line_start_date
  ,cast(claim_line_end_date as date) as claim_line_end_date
  ,cast(bill_type_code as varchar) as bill_type_code
  ,cast(bill_type_description as varchar) as bill_type_description
  ,cast(place_of_service_code as varchar) as place_of_service_code
  ,cast(place_of_service_description as varchar) as place_of_service_description
  ,cast(revenue_center_code as varchar) as revenue_center_code
  ,cast(revenue_center_description as varchar) as revenue_center_description
  ,cast(paid_date as date) as paid_date
  ,cast(paid_amount as numeric(38,2)) as paid_amount
  ,cast(hcpcs_code as varchar) as hcpcs_code
  ,cast(hcpcs_modifier_1 as varchar) as hcpcs_modifier_1
  ,cast(hcpcs_modifier_2 as varchar) as hcpcs_modifier_2
  ,cast(hcpcs_modifier_3 as varchar) as hcpcs_modifier_3
  ,cast(hcpcs_modifier_4 as varchar) as hcpcs_modifier_4
  ,cast(hcpcs_modifier_5 as varchar) as hcpcs_modifier_5
  ,cast(rendering_npi as varchar) as rendering_npi
  ,cast(billing_npi as varchar) as billing_npi
  ,cast(facility_npi as varchar) as facility_npi
  ,cast(discharge_disposition_code as varchar) as discharge_disposition_code
  ,cast(discharge_disposition_description as varchar) as discharge_disposition_description
  ,cast(charge_amount as numeric(38,2)) as charge_amount
  ,cast(data_source as varchar) as data_source
from {{ ref('encounter_claim_line_stage')}}
