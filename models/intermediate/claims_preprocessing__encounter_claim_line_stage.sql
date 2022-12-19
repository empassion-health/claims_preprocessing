-------------------------------------------------------------------------------
-- Author       Thu Xuan Vu
-- Created      June 2022
-- Purpose      A copy of the claims input layer with enhancements(terminology, merged claims, and encounter types).  This will power the core layer.
-- Notes        Created this table to allow for validation and as an easier way to pass through claim fields from input layer to output layer.
-------------------------------------------------------------------------------
-- Modification History
-- 09/28/2022 Thu Xuan Vu
--    Changed references of merge_claim_id to claim_id
-- 11/01/2022 Thu Xuan Vu
--    Updated table/model name to encounter_claim_line
-------------------------------------------------------------------------------
{{ config(
    tags=["medical_claim"]
    ,enabled=var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))
) }}

select 
  {{ cast_string_or_varchar('m.claim_id') }} as claim_id
  ,{{ cast_string_or_varchar('m.claim_line_number') }} as claim_line_number
  ,{{ cast_string_or_varchar('m.claim_type') }} as claim_type
  ,{{ cast_string_or_varchar('c.encounter_id') }} as encounter_id
  ,{{ cast_string_or_varchar('e.encounter_type') }} as encounter_type
  ,{{ cast_string_or_varchar('m.patient_id') }} as patient_id
  ,{{ cast_string_or_varchar('m.member_id') }} as member_id
  ,cast(m.claim_start_date as date) as claim_start_date
  ,cast(m.claim_end_date as date) as claim_end_date
  ,cast(m.claim_line_start_date as date) as claim_line_start_date
  ,cast(m.claim_line_end_date as date) as claim_line_end_date
  ,cast(m.admission_date as date) as admission_date
  ,cast(m.discharge_date as date) as discharge_date
  ,{{ cast_string_or_varchar('m.admit_source_code') }} as admit_source_code
  ,case when m.admit_type_code = '4' then cast(asrc.newborn_description as varchar) else cast(asrc.admit_source_description as varchar) end as admit_source_description
  ,{{ cast_string_or_varchar('m.admit_type_code') }} as admit_type_code
  ,{{ cast_string_or_varchar('at.admit_type_description') }} as admit_type_description
  ,{{ cast_string_or_varchar('m.discharge_disposition_code') }} as discharge_disposition_code
  ,{{ cast_string_or_varchar('dd.discharge_disposition_description') }} as discharge_disposition_description
  ,{{ cast_string_or_varchar('m.place_of_service_code') }} as place_of_service_code 
  ,{{ cast_string_or_varchar('pos.place_of_service_description') }} as place_of_service_description
  ,{{ cast_string_or_varchar('m.bill_type_code') }} as bill_type_code
  ,cast(null as varchar) as bill_type_description
  ,{{ cast_string_or_varchar('m.ms_drg_code') }} as ms_drg_code
  ,{{ cast_string_or_varchar('msdrg.ms_drg_description') }} as ms_drg_description
  ,{{ cast_string_or_varchar('m.revenue_center_code') }} as revenue_center_code
  ,{{ cast_string_or_varchar('rev.revenue_center_description') }} as revenue_center_description
  ,cast(m.service_unit_quantity as int) as service_unit_quantity
  ,{{ cast_string_or_varchar('m.hcpcs_code') }} as hcpcs_code
  ,{{ cast_string_or_varchar('m.hcpcs_modifier_1') }} as hcpcs_modifier_1
  ,{{ cast_string_or_varchar('m.hcpcs_modifier_2') }} as hcpcs_modifier_2
  ,{{ cast_string_or_varchar('m.hcpcs_modifier_3') }} as hcpcs_modifier_3
  ,{{ cast_string_or_varchar('m.hcpcs_modifier_4') }} as hcpcs_modifier_4
  ,{{ cast_string_or_varchar('m.hcpcs_modifier_5') }} as hcpcs_modifier_5
  ,{{ cast_string_or_varchar('m.rendering_npi') }} as rendering_npi
  ,{{ cast_string_or_varchar('m.billing_npi') }} as billing_npi
  ,{{ cast_string_or_varchar('m.facility_npi') }} as facility_npi
  ,cast(NULL as varchar) as facility_name
  ,cast(m.paid_date as date) as paid_date
  ,cast(m.paid_amount as numeric(38,4)) as paid_amount
  ,cast(m.allowed_amount as numeric(38,4)) as allowed_amount
  ,cast(m.charge_amount as numeric(38,4)) as charge_amount
  ,{{ cast_string_or_varchar('m.diagnosis_code_1') }} as diagnosis_code_1
  ,{{ cast_string_or_varchar('m.diagnosis_code_2') }} as diagnosis_code_2
  ,{{ cast_string_or_varchar('m.diagnosis_code_3') }} as diagnosis_code_3
  ,{{ cast_string_or_varchar('m.diagnosis_code_4') }} as diagnosis_code_4
  ,{{ cast_string_or_varchar('m.diagnosis_code_5') }} as diagnosis_code_5
  ,{{ cast_string_or_varchar('m.diagnosis_code_6') }} as diagnosis_code_6
  ,{{ cast_string_or_varchar('m.diagnosis_code_7') }} as diagnosis_code_7
  ,{{ cast_string_or_varchar('m.diagnosis_code_8') }} as diagnosis_code_8
  ,{{ cast_string_or_varchar('m.diagnosis_code_9') }} as diagnosis_code_9
  ,{{ cast_string_or_varchar('m.diagnosis_code_10') }} as diagnosis_code_10
  ,{{ cast_string_or_varchar('m.diagnosis_code_11') }} as diagnosis_code_11
  ,{{ cast_string_or_varchar('m.diagnosis_code_12') }} as diagnosis_code_12
  ,{{ cast_string_or_varchar('m.diagnosis_code_13') }} as diagnosis_code_13
  ,{{ cast_string_or_varchar('m.diagnosis_code_14') }} as diagnosis_code_14
  ,{{ cast_string_or_varchar('m.diagnosis_code_15') }} as diagnosis_code_15
  ,{{ cast_string_or_varchar('m.diagnosis_code_16') }} as diagnosis_code_16
  ,{{ cast_string_or_varchar('m.diagnosis_code_17') }} as diagnosis_code_17
  ,{{ cast_string_or_varchar('m.diagnosis_code_18') }} as diagnosis_code_18
  ,{{ cast_string_or_varchar('m.diagnosis_code_19') }} as diagnosis_code_19
  ,{{ cast_string_or_varchar('m.diagnosis_code_20') }} as diagnosis_code_20
  ,{{ cast_string_or_varchar('m.diagnosis_code_21') }} as diagnosis_code_21
  ,{{ cast_string_or_varchar('m.diagnosis_code_22') }} as diagnosis_code_22
  ,{{ cast_string_or_varchar('m.diagnosis_code_23') }} as diagnosis_code_23
  ,{{ cast_string_or_varchar('m.diagnosis_code_24') }} as diagnosis_code_24
  ,{{ cast_string_or_varchar('m.diagnosis_code_25') }} as diagnosis_code_25
  ,{{ cast_string_or_varchar('m.diagnosis_poa_1') }} as diagnosis_poa_1
  ,{{ cast_string_or_varchar('m.diagnosis_poa_2') }} as diagnosis_poa_2
  ,{{ cast_string_or_varchar('m.diagnosis_poa_3') }} as diagnosis_poa_3
  ,{{ cast_string_or_varchar('m.diagnosis_poa_4') }} as diagnosis_poa_4
  ,{{ cast_string_or_varchar('m.diagnosis_poa_5') }} as diagnosis_poa_5
  ,{{ cast_string_or_varchar('m.diagnosis_poa_6') }} as diagnosis_poa_6
  ,{{ cast_string_or_varchar('m.diagnosis_poa_7') }} as diagnosis_poa_7
  ,{{ cast_string_or_varchar('m.diagnosis_poa_8') }} as diagnosis_poa_8
  ,{{ cast_string_or_varchar('m.diagnosis_poa_9') }} as diagnosis_poa_9
  ,{{ cast_string_or_varchar('m.diagnosis_poa_10') }} as diagnosis_poa_10
  ,{{ cast_string_or_varchar('m.diagnosis_poa_11') }} as diagnosis_poa_11
  ,{{ cast_string_or_varchar('m.diagnosis_poa_12') }} as diagnosis_poa_12
  ,{{ cast_string_or_varchar('m.diagnosis_poa_13') }} as diagnosis_poa_13
  ,{{ cast_string_or_varchar('m.diagnosis_poa_14') }} as diagnosis_poa_14
  ,{{ cast_string_or_varchar('m.diagnosis_poa_15') }} as diagnosis_poa_15
  ,{{ cast_string_or_varchar('m.diagnosis_poa_16') }} as diagnosis_poa_16
  ,{{ cast_string_or_varchar('m.diagnosis_poa_17') }} as diagnosis_poa_17
  ,{{ cast_string_or_varchar('m.diagnosis_poa_18') }} as diagnosis_poa_18
  ,{{ cast_string_or_varchar('m.diagnosis_poa_19') }} as diagnosis_poa_19
  ,{{ cast_string_or_varchar('m.diagnosis_poa_20') }} as diagnosis_poa_20
  ,{{ cast_string_or_varchar('m.diagnosis_poa_21') }} as diagnosis_poa_21
  ,{{ cast_string_or_varchar('m.diagnosis_poa_22') }} as diagnosis_poa_22
  ,{{ cast_string_or_varchar('m.diagnosis_poa_23') }} as diagnosis_poa_23
  ,{{ cast_string_or_varchar('m.diagnosis_poa_24') }} as diagnosis_poa_24
  ,{{ cast_string_or_varchar('m.diagnosis_poa_25') }} as diagnosis_poa_25
  ,{{ cast_string_or_varchar('m.diagnosis_code_type') }} as diagnosis_code_type
  ,{{ cast_string_or_varchar('m.procedure_code_type') }} as procedure_code_type
  ,{{ cast_string_or_varchar('m.procedure_code_1') }} as procedure_code_1
  ,{{ cast_string_or_varchar('m.procedure_code_2') }} as procedure_code_2
  ,{{ cast_string_or_varchar('m.procedure_code_3') }} as procedure_code_3
  ,{{ cast_string_or_varchar('m.procedure_code_4') }} as procedure_code_4
  ,{{ cast_string_or_varchar('m.procedure_code_5') }} as procedure_code_5
  ,{{ cast_string_or_varchar('m.procedure_code_6') }} as procedure_code_6
  ,{{ cast_string_or_varchar('m.procedure_code_7') }} as procedure_code_7
  ,{{ cast_string_or_varchar('m.procedure_code_8') }} as procedure_code_8
  ,{{ cast_string_or_varchar('m.procedure_code_9') }} as procedure_code_9
  ,{{ cast_string_or_varchar('m.procedure_code_10') }} as procedure_code_10
  ,{{ cast_string_or_varchar('m.procedure_code_11') }} as procedure_code_11
  ,{{ cast_string_or_varchar('m.procedure_code_12') }} as procedure_code_12
  ,{{ cast_string_or_varchar('m.procedure_code_13') }} as procedure_code_13
  ,{{ cast_string_or_varchar('m.procedure_code_14') }} as procedure_code_14
  ,{{ cast_string_or_varchar('m.procedure_code_15') }} as procedure_code_15
  ,{{ cast_string_or_varchar('m.procedure_code_16') }} as procedure_code_16
  ,{{ cast_string_or_varchar('m.procedure_code_17') }} as procedure_code_17
  ,{{ cast_string_or_varchar('m.procedure_code_18') }} as procedure_code_18
  ,{{ cast_string_or_varchar('m.procedure_code_19') }} as procedure_code_19
  ,{{ cast_string_or_varchar('m.procedure_code_20') }} as procedure_code_20
  ,{{ cast_string_or_varchar('m.procedure_code_21') }} as procedure_code_21
  ,{{ cast_string_or_varchar('m.procedure_code_22') }} as procedure_code_22
  ,{{ cast_string_or_varchar('m.procedure_code_23') }} as procedure_code_23
  ,{{ cast_string_or_varchar('m.procedure_code_24') }} as procedure_code_24
  ,{{ cast_string_or_varchar('m.procedure_code_25') }} as procedure_code_25
  ,{{ cast_string_or_varchar('m.procedure_date_1') }} as procedure_date_1
  ,{{ cast_string_or_varchar('m.procedure_date_2') }} as procedure_date_2
  ,{{ cast_string_or_varchar('m.procedure_date_3') }} as procedure_date_3
  ,{{ cast_string_or_varchar('m.procedure_date_4') }} as procedure_date_4
  ,{{ cast_string_or_varchar('m.procedure_date_5') }} as procedure_date_5
  ,{{ cast_string_or_varchar('m.procedure_date_6') }} as procedure_date_6
  ,{{ cast_string_or_varchar('m.procedure_date_7') }} as procedure_date_7
  ,{{ cast_string_or_varchar('m.procedure_date_8') }} as procedure_date_8
  ,{{ cast_string_or_varchar('m.procedure_date_9') }} as procedure_date_9
  ,{{ cast_string_or_varchar('m.procedure_date_10') }} as procedure_date_10
  ,{{ cast_string_or_varchar('m.procedure_date_11') }} as procedure_date_11
  ,{{ cast_string_or_varchar('m.procedure_date_12') }} as procedure_date_12
  ,{{ cast_string_or_varchar('m.procedure_date_13') }} as procedure_date_13
  ,{{ cast_string_or_varchar('m.procedure_date_14') }} as procedure_date_14
  ,{{ cast_string_or_varchar('m.procedure_date_15') }} as procedure_date_15
  ,{{ cast_string_or_varchar('m.procedure_date_16') }} as procedure_date_16
  ,{{ cast_string_or_varchar('m.procedure_date_17') }} as procedure_date_17
  ,{{ cast_string_or_varchar('m.procedure_date_18') }} as procedure_date_18
  ,{{ cast_string_or_varchar('m.procedure_date_19') }} as procedure_date_19
  ,{{ cast_string_or_varchar('m.procedure_date_20') }} as procedure_date_20
  ,{{ cast_string_or_varchar('m.procedure_date_21') }} as procedure_date_21
  ,{{ cast_string_or_varchar('m.procedure_date_22') }} as procedure_date_22
  ,{{ cast_string_or_varchar('m.procedure_date_23') }} as procedure_date_23
  ,{{ cast_string_or_varchar('m.procedure_date_24') }} as procedure_date_24
  ,{{ cast_string_or_varchar('m.procedure_date_25') }} as procedure_date_25
  ,{{ cast_string_or_varchar('data_source') }} as data_source
from {{ var('medical_claim')}} m
inner join {{ ref('claims_preprocessing__encounter_type_union')}} e
	on m.claim_id = e.claim_id
inner join {{ ref('claims_preprocessing__encounter_claim_crosswalk')}} c
	on e.claim_id = c.claim_id
left join {{ ref('terminology__admit_type')}} at
	on m.admit_type_code = at.admit_type_code
left join {{ ref('terminology__admit_source')}} asrc
	on m.admit_source_code = asrc.admit_source_code
left join {{ ref('terminology__revenue_center')}} rev
	on m.revenue_center_code = rev.revenue_center_code
left join {{ ref('terminology__place_of_service')}} pos
	on m.place_of_service_code = pos.place_of_service_code
left join {{ ref('terminology__discharge_disposition')}} dd
	on m.discharge_disposition_code = dd.discharge_disposition_code
left join {{ ref('terminology__ms_drg')}} msdrg
	on m.ms_drg_code = msdrg.ms_drg_code

{% if target.type in ('redshift') -%}

where isnull(m.revenue_center_code,'') <> '0001'


{%- elif target.type in ('snowflake') -%}


where ifnull(m.revenue_center_code,'') <> '0001'


{%- else -%}
{%- endif %}