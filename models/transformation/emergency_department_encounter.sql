---------------------------------------------------------------------------------------------------------
-- Author       Thu Xuan Vu
-- Created      November 2022
-- Purpose      Map claims to emergency department that match the following criteria:
--              Institution
--                1) Contains emergency department revenue code
--                2) Does not contain room and board code on same claim
--              Professional
--                1) Place of service = 23
-- Notes
---------------------------------------------------------------------------------------------------------
-- Modification History
---------------------------------------------------------------------------------------------------------
{{ config(
    tags=["medical_claim"]
) }}


with claim_emergency_department_eligibility as(
  select distinct 
      claim_type
    , max(med.revenue_center_code) as revenue_center_code
    , max(bill_type_code) as bill_type_code
    , max(ms_drg) as ms_drg
    , claim_id
  from {{ var('medical_claim')}} med
  where med.revenue_center_code in ('0450','0451','0452','0459','0981')
  group by 
    claim_type
    , claim_id
)
, room_and_board_claims as(
  select 
     med.revenue_center_code
    , rev.description
    , claim_id
  from {{ var('medical_claim')}} med
  inner join {{ source('tuva_terminology','revenue_center_code')}} rev
    on med.revenue_center_code = rev.code
   where med.revenue_center_code in ('0100','0101','0110','0111','0112','0113','0114','0116','0117','0118','0119'
  ,'0120','0121','0122','0123','0124','0126','0127','0128','0129','0130','0131','0132','0133','0134'
  ,'0136','0137','0138','0139','0140','0141','0142','0143','0144','0146','0147','0148','0149','0150'
  ,'0151','0152','0153','0154','0156','0157','0158','0159','0160','0164','0167','0169','0170','0171'
  ,'0172','0173','0174','0179','0190','0191','0192','0193','0194','0199','0200','0201','0202','0203'
  ,'0204','0206','0207','0208','0209','0210','0211','0212','0213','0214','0219','1000','1001','1002')
)

select 
      e.claim_type
    , 'emergency department' as encounter_type
    , rev.code as revenue_center_code
    , rev.description as revenue_center_description
    , e.bill_type_code
    , e.ms_drg
    , e.claim_id 
    , null as place_of_service_code
    , null as place_of_service_description
from claim_emergency_department_eligibility e
inner join {{ source('tuva_terminology','revenue_center_code')}} rev
  on e.revenue_center_code = rev.code
left join room_and_board_claims r
    on e.claim_id = r.claim_id
where r.claim_id is null

union all 

select distinct
      med.claim_type
    , 'emergency department' as encounter_type
    , null as revenue_center_code
    , null as revenue_center_description
    , null as bill_type_code
    , null as ms_drg
    , med.claim_id 
    , pos.code as place_of_service_code
    , pos.description as place_of_service_description
from {{ var('medical_claim')}} med
   inner join {{ source('tuva_terminology','place_of_service')}} pos
    on med.place_of_service_code = pos.code
where pos.code = '23'