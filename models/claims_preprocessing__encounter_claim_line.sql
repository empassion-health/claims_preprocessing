-------------------------------------------------------------------------------
-- Author       Thu Xuan Vu
-- Created      June 2022
-- Purpose      Populate medical claim line level detail.
-------------------------------------------------------------------------------
-- Modification History
-- 11/01/2022 Thu Xuan Vu
--    Updated table/model name to encounter_claim_line
-------------------------------------------------------------------------------
{{ config(enabled=var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))) }}

select 
  {{ cast_string_or_varchar('encounter_id') }} as encounter_id
  ,{{ cast_string_or_varchar('encounter_type') }} as encounter_type
  ,{{ cast_string_or_varchar('patient_id') }} as patient_id
  ,{{ cast_string_or_varchar('member_id') }} as member_id
  ,{{ cast_string_or_varchar('claim_id') }} as claim_id
  ,cast(claim_line_number as int) as claim_line_number
  ,{{ cast_string_or_varchar('claim_type') }} as claim_type
  ,cast(claim_start_date as date) as claim_start_date
  ,cast(claim_end_date as date) as claim_end_date
  ,cast(claim_line_start_date as date) as claim_line_start_date
  ,cast(claim_line_end_date as date) as claim_line_end_date
  ,{{ cast_string_or_varchar('bill_type_code') }} as bill_type_code
  ,{{ cast_string_or_varchar('bill_type_description') }} as bill_type_description
  ,{{ cast_string_or_varchar('place_of_service_code') }} as place_of_service_code
  ,{{ cast_string_or_varchar('place_of_service_description') }} as place_of_service_description
  ,{{ cast_string_or_varchar('revenue_center_code') }} as revenue_center_code
  ,{{ cast_string_or_varchar('revenue_center_description') }} as revenue_center_description
  ,cast(service_unit_quantity as int) as service_unit_quantity
  ,{{ cast_string_or_varchar('hcpcs_code') }} as hcpcs_code
  ,{{ cast_string_or_varchar('hcpcs_modifier_1') }} as hcpcs_modifier_1
  ,{{ cast_string_or_varchar('hcpcs_modifier_2') }} as hcpcs_modifier_2
  ,{{ cast_string_or_varchar('hcpcs_modifier_3') }} as hcpcs_modifier_3
  ,{{ cast_string_or_varchar('hcpcs_modifier_4') }} as hcpcs_modifier_4
  ,{{ cast_string_or_varchar('hcpcs_modifier_5') }} as hcpcs_modifier_5
  ,{{ cast_string_or_varchar('rendering_npi') }} as rendering_npi
  ,{{ cast_string_or_varchar('billing_npi') }} as billing_npi
  ,{{ cast_string_or_varchar('facility_npi') }} as facility_npi
  ,{{ cast_string_or_varchar('discharge_disposition_code') }} as discharge_disposition_code
  ,{{ cast_string_or_varchar('discharge_disposition_description') }} as discharge_disposition_description
  ,cast(paid_date as date) as paid_date
  ,cast(paid_amount as numeric(38,2)) as paid_amount
  ,cast(allowed_amount as numeric(38,2)) as allowed_amount
  ,cast(charge_amount as numeric(38,2)) as charge_amount
  ,{{ cast_string_or_varchar('data_source') }} as data_source
from {{ ref('claims_preprocessing__encounter_claim_line_stage')}}
