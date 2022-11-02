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
) }}

select 
  cast(m.claim_id as varchar) as claim_id
  ,cast(m.claim_line_number as varchar) as claim_line_number
  ,cast(m.claim_type as varchar) as claim_type
  ,cast(c.encounter_id as varchar) as encounter_id
  ,cast(e.encounter_type as varchar) as encounter_type
  ,cast(m.patient_id as varchar) as patient_id
  ,cast(m.claim_start_date as date) as claim_start_date
  ,cast(m.claim_end_date as date) as claim_end_date
  ,cast(m.claim_line_start_date as date) as claim_line_start_date
  ,cast(m.claim_line_end_date as date) as claim_line_end_date
  ,cast(m.bill_type_code as varchar) as bill_type_code
  ,cast(null as varchar) as bill_type_description
  ,cast(m.place_of_service_code as varchar) as place_of_service_code 
  ,cast(pos.description as varchar) as place_of_service_description
  ,cast(m.revenue_center_code as varchar) as revenue_center_code
  ,cast(rev.description as varchar) as revenue_center_description
  ,cast(m.admit_type_code as varchar) as admit_type_code
  ,cast(at.description as varchar) as admit_type_description
  ,cast(m.admit_source_code as varchar) as admit_source_code
  ,cast(asrc.description as varchar) as admit_source_description
  ,cast(m.paid_date as date) as paid_date
  ,cast(m.paid_amount as numeric(38,4)) as paid_amount
  ,cast(m.hcpcs_code as varchar) as hcpcs_code
  ,cast(m.hcpcs_modifier_1 as varchar) as hcpcs_modifier_1
  ,cast(m.hcpcs_modifier_2 as varchar) as hcpcs_modifier_2
  ,cast(m.hcpcs_modifier_3 as varchar) as hcpcs_modifier_3
  ,cast(m.hcpcs_modifier_4 as varchar) as hcpcs_modifier_4
  ,cast(m.hcpcs_modifier_5 as varchar) as hcpcs_modifier_5
  ,cast(m.rendering_npi as varchar) as rendering_npi
  ,cast(m.billing_npi as varchar) as billing_npi
  ,cast(m.facility_npi as varchar) as facility_npi
  ,cast(m.ms_drg as varchar) as ms_drg
  ,cast(m.discharge_disposition_code as varchar) as discharge_disposition_code
  ,cast(dd.description as varchar) as discharge_disposition_description
  ,cast(m.charge_amount as numeric(38,4)) as charge_amount
  ,cast(m.diagnosis_code_1 as varchar) as diagnosis_code_1
  ,cast(m.diagnosis_code_2 as varchar) as diagnosis_code_2
  ,cast(m.diagnosis_code_3 as varchar) as diagnosis_code_3
  ,cast(m.diagnosis_code_4 as varchar) as diagnosis_code_4
  ,cast(m.diagnosis_code_5 as varchar) as diagnosis_code_5
  ,cast(m.diagnosis_code_6 as varchar) as diagnosis_code_6
  ,cast(m.diagnosis_code_7 as varchar) as diagnosis_code_7
  ,cast(m.diagnosis_code_8 as varchar) as diagnosis_code_8
  ,cast(m.diagnosis_code_9 as varchar) as diagnosis_code_9
  ,cast(m.diagnosis_code_10 as varchar) as diagnosis_code_10
  ,cast(m.diagnosis_code_11 as varchar) as diagnosis_code_11
  ,cast(m.diagnosis_code_12 as varchar) as diagnosis_code_12
  ,cast(m.diagnosis_code_13 as varchar) as diagnosis_code_13
  ,cast(m.diagnosis_code_14 as varchar) as diagnosis_code_14
  ,cast(m.diagnosis_code_15 as varchar) as diagnosis_code_15
  ,cast(m.diagnosis_code_16 as varchar) as diagnosis_code_16
  ,cast(m.diagnosis_code_17 as varchar) as diagnosis_code_17
  ,cast(m.diagnosis_code_18 as varchar) as diagnosis_code_18
  ,cast(m.diagnosis_code_19 as varchar) as diagnosis_code_19
  ,cast(m.diagnosis_code_20 as varchar) as diagnosis_code_20
  ,cast(m.diagnosis_code_21 as varchar) as diagnosis_code_21
  ,cast(m.diagnosis_code_22 as varchar) as diagnosis_code_22
  ,cast(m.diagnosis_code_23 as varchar) as diagnosis_code_23
  ,cast(m.diagnosis_code_24 as varchar) as diagnosis_code_24
  ,cast(m.diagnosis_code_25 as varchar) as diagnosis_code_25
  ,cast(m.diagnosis_poa_1 as varchar) as diagnosis_poa_1
  ,cast(m.diagnosis_poa_2 as varchar) as diagnosis_poa_2
  ,cast(m.diagnosis_poa_3 as varchar) as diagnosis_poa_3
  ,cast(m.diagnosis_poa_4 as varchar) as diagnosis_poa_4
  ,cast(m.diagnosis_poa_5 as varchar) as diagnosis_poa_5
  ,cast(m.diagnosis_poa_6 as varchar) as diagnosis_poa_6
  ,cast(m.diagnosis_poa_7 as varchar) as diagnosis_poa_7
  ,cast(m.diagnosis_poa_8 as varchar) as diagnosis_poa_8
  ,cast(m.diagnosis_poa_9 as varchar) as diagnosis_poa_9
  ,cast(m.diagnosis_poa_10 as varchar) as diagnosis_poa_10
  ,cast(m.diagnosis_poa_11 as varchar) as diagnosis_poa_11
  ,cast(m.diagnosis_poa_12 as varchar) as diagnosis_poa_12
  ,cast(m.diagnosis_poa_13 as varchar) as diagnosis_poa_13
  ,cast(m.diagnosis_poa_14 as varchar) as diagnosis_poa_14
  ,cast(m.diagnosis_poa_15 as varchar) as diagnosis_poa_15
  ,cast(m.diagnosis_poa_16 as varchar) as diagnosis_poa_16
  ,cast(m.diagnosis_poa_17 as varchar) as diagnosis_poa_17
  ,cast(m.diagnosis_poa_18 as varchar) as diagnosis_poa_18
  ,cast(m.diagnosis_poa_19 as varchar) as diagnosis_poa_19
  ,cast(m.diagnosis_poa_20 as varchar) as diagnosis_poa_20
  ,cast(m.diagnosis_poa_21 as varchar) as diagnosis_poa_21
  ,cast(m.diagnosis_poa_22 as varchar) as diagnosis_poa_22
  ,cast(m.diagnosis_poa_23 as varchar) as diagnosis_poa_23
  ,cast(m.diagnosis_poa_24 as varchar) as diagnosis_poa_24
  ,cast(m.diagnosis_poa_25 as varchar) as diagnosis_poa_25
  ,cast(m.diagnosis_code_type as varchar) as diagnosis_code_type
  ,cast(m.procedure_code_type as varchar) as procedure_code_type
  ,cast(m.procedure_code_1 as varchar) as procedure_code_1
  ,cast(m.procedure_code_2 as varchar) as procedure_code_2
  ,cast(m.procedure_code_3 as varchar) as procedure_code_3
  ,cast(m.procedure_code_4 as varchar) as procedure_code_4
  ,cast(m.procedure_code_5 as varchar) as procedure_code_5
  ,cast(m.procedure_code_6 as varchar) as procedure_code_6
  ,cast(m.procedure_code_7 as varchar) as procedure_code_7
  ,cast(m.procedure_code_8 as varchar) as procedure_code_8
  ,cast(m.procedure_code_9 as varchar) as procedure_code_9
  ,cast(m.procedure_code_10 as varchar) as procedure_code_10
  ,cast(m.procedure_code_11 as varchar) as procedure_code_11
  ,cast(m.procedure_code_12 as varchar) as procedure_code_12
  ,cast(m.procedure_code_13 as varchar) as procedure_code_13
  ,cast(m.procedure_code_14 as varchar) as procedure_code_14
  ,cast(m.procedure_code_15 as varchar) as procedure_code_15
  ,cast(m.procedure_code_16 as varchar) as procedure_code_16
  ,cast(m.procedure_code_17 as varchar) as procedure_code_17
  ,cast(m.procedure_code_18 as varchar) as procedure_code_18
  ,cast(m.procedure_code_19 as varchar) as procedure_code_19
  ,cast(m.procedure_code_20 as varchar) as procedure_code_20
  ,cast(m.procedure_code_21 as varchar) as procedure_code_21
  ,cast(m.procedure_code_22 as varchar) as procedure_code_22
  ,cast(m.procedure_code_23 as varchar) as procedure_code_23
  ,cast(m.procedure_code_24 as varchar) as procedure_code_24
  ,cast(m.procedure_code_25 as varchar) as procedure_code_25
  ,cast(m.procedure_date_1 as varchar) as procedure_date_1
  ,cast(m.procedure_date_2 as varchar) as procedure_date_2
  ,cast(m.procedure_date_3 as varchar) as procedure_date_3
  ,cast(m.procedure_date_4 as varchar) as procedure_date_4
  ,cast(m.procedure_date_5 as varchar) as procedure_date_5
  ,cast(m.procedure_date_6 as varchar) as procedure_date_6
  ,cast(m.procedure_date_7 as varchar) as procedure_date_7
  ,cast(m.procedure_date_8 as varchar) as procedure_date_8
  ,cast(m.procedure_date_9 as varchar) as procedure_date_9
  ,cast(m.procedure_date_10 as varchar) as procedure_date_10
  ,cast(m.procedure_date_11 as varchar) as procedure_date_11
  ,cast(m.procedure_date_12 as varchar) as procedure_date_12
  ,cast(m.procedure_date_13 as varchar) as procedure_date_13
  ,cast(m.procedure_date_14 as varchar) as procedure_date_14
  ,cast(m.procedure_date_15 as varchar) as procedure_date_15
  ,cast(m.procedure_date_16 as varchar) as procedure_date_16
  ,cast(m.procedure_date_17 as varchar) as procedure_date_17
  ,cast(m.procedure_date_18 as varchar) as procedure_date_18
  ,cast(m.procedure_date_19 as varchar) as procedure_date_19
  ,cast(m.procedure_date_20 as varchar) as procedure_date_20
  ,cast(m.procedure_date_21 as varchar) as procedure_date_21
  ,cast(m.procedure_date_22 as varchar) as procedure_date_22
  ,cast(m.procedure_date_23 as varchar) as procedure_date_23
  ,cast(m.procedure_date_24 as varchar) as procedure_date_24
  ,cast(m.procedure_date_25 as varchar) as procedure_date_25
  ,cast('{{ var('source_name')}}' as varchar) as data_source
from {{ var('medical_claim')}} m
inner join {{ ref('encounter_type_union')}} e
	on m.claim_id = e.claim_id
inner join {{ ref('encounter_claim_crosswalk')}} c
	on e.claim_id = c.claim_id
left join {{ source('tuva_terminology','admit_type')}} at
	on m.admit_type_code = at.code
left join {{ source('tuva_terminology','admit_source')}} asrc
	on m.admit_source_code = asrc.code
left join {{ source('tuva_terminology','revenue_center_code')}} rev
	on m.revenue_center_code = rev.code
left join {{ source('tuva_terminology','place_of_service')}} pos
	on m.place_of_service_code = pos.code
left join {{ source('tuva_terminology','discharge_disposition')}} dd
	on m.discharge_disposition_code = dd.code
where ifnull(m.revenue_center_code,'') <> '0001'