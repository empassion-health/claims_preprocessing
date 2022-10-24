---------------------------------------------------------------------------------------------------------
-- Author       Thu Xuan Vu
-- Created      May 2022
-- Purpose      Map claims to encounter types at the line level.  Institutional claims use rev code or 
--                  bill type, professional claims use place of service.
-- Notes
---------------------------------------------------------------------------------------------------------
-- Modification History
--
-- 06/15/2022  Thu Xuan Vu
--      Fixed spelling of treatment and psychiatric
-- 09/27/2022  Thu Xuan Vu
--      Changed maping to the claim level.
--      Using MIN() since not all claims may start with 1
--      Removed the creation of a new encounter ID since a claim can no longer split into 
--          different encounter types.
--      Revenue code 001 no longer omitted since it cannot be split into a seperate claim
--      Added row_number for downstream ordering
--      Added billing_npi for use in professional merging
-- 10/14/2022 Thu Xuan Vu
--      Added leading zero to place of service codes
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
, encounter_type_mapping as(
  select
    case
      when trim(revenue_center_code) in ('0450','0459') then 'emergency department'
      when left(trim(bill_type_code),2) = '11' then 'acute inpatient'
      when left(trim(bill_type_code),2) = '12' then 'acute inpatient'
      when left(trim(bill_type_code),2) = '13' then 'outpatient'
      when left(trim(bill_type_code),2) = '14' then 'other'
      when left(trim(bill_type_code),2) = '15' then 'acute inpatient'
      when left(trim(bill_type_code),2) = '16' then 'acute inpatient'
      when left(trim(bill_type_code),2) = '17' then 'acute inpatient'
      when left(trim(bill_type_code),2) = '18' then 'acute inpatient'
      when left(trim(bill_type_code),2) = '21' then 'skilled nursing facility'
      when left(trim(bill_type_code),2) = '22' then 'skilled nursing facility'
      when left(trim(bill_type_code),2) = '23' then 'skilled nursing facility'
      when left(trim(bill_type_code),2) = '24' then 'skilled nursing facility'
      when left(trim(bill_type_code),2) = '25' then 'skilled nursing facility'
      when left(trim(bill_type_code),2) = '26' then 'skilled nursing facility'
      when left(trim(bill_type_code),2) = '27' then 'skilled nursing facility'
      when left(trim(bill_type_code),2) = '28' then 'skilled nursing facility'
      when left(trim(bill_type_code),2) = '31' then 'home health'
      when left(trim(bill_type_code),2) = '32' then 'home health'
      when left(trim(bill_type_code),2) = '33' then 'home health'
      when left(trim(bill_type_code),2) = '34' then 'home health'
      when left(trim(bill_type_code),2) = '35' then 'home health'
      when left(trim(bill_type_code),2) = '36' then 'home health'
      when left(trim(bill_type_code),2) = '37' then 'home health'
      when left(trim(bill_type_code),2) = '38' then 'home health'
      when left(trim(bill_type_code),2) = '41' then 'acute inpatient'
      when left(trim(bill_type_code),2) = '42' then 'acute inpatient'
      when left(trim(bill_type_code),2) = '43' then 'acute inpatient'
      when left(trim(bill_type_code),2) = '44' then 'acute inpatient'
      when left(trim(bill_type_code),2) = '45' then 'acute inpatient'
      when left(trim(bill_type_code),2) = '46' then 'acute inpatient'
      when left(trim(bill_type_code),2) = '47' then 'acute inpatient'
      when left(trim(bill_type_code),2) = '48' then 'acute inpatient'
      when left(trim(bill_type_code),2) = '51' then 'other'
      when left(trim(bill_type_code),2) = '52' then 'other'
      when left(trim(bill_type_code),2) = '53' then 'other'
      when left(trim(bill_type_code),2) = '54' then 'other'
      when left(trim(bill_type_code),2) = '55' then 'other'
      when left(trim(bill_type_code),2) = '56' then 'other'
      when left(trim(bill_type_code),2) = '57' then 'other'
      when left(trim(bill_type_code),2) = '58' then 'other'
      when left(trim(bill_type_code),2) = '61' then 'other'
      when left(trim(bill_type_code),2) = '62' then 'other'
      when left(trim(bill_type_code),2) = '63' then 'other'
      when left(trim(bill_type_code),2) = '64' then 'other'
      when left(trim(bill_type_code),2) = '65' then 'other'
      when left(trim(bill_type_code),2) = '66' then 'other'
      when left(trim(bill_type_code),2) = '67' then 'other'
      when left(trim(bill_type_code),2) = '68' then 'other'
      when left(trim(bill_type_code),2) = '71' then 'outpatient'
      when left(trim(bill_type_code),2) = '72' then 'dialysis center'
      when left(trim(bill_type_code),2) = '73' then 'other'
      when left(trim(bill_type_code),2) = '74' then 'outpatient rehabilitation'
      when left(trim(bill_type_code),2) = '75' then 'outpatient rehabilitation'
      when left(trim(bill_type_code),2) = '76' then 'mental health center'
      when left(trim(bill_type_code),2) = '79' then 'outpatient'
      when left(trim(bill_type_code),2) = '81' then 'hospice'
      when left(trim(bill_type_code),2) = '82' then 'hospice'
      when left(trim(bill_type_code),2) = '83' then 'ambulatory surgical center'
      when left(trim(bill_type_code),2) = '84' then 'acute inpatient'
      when left(trim(bill_type_code),2) = '85' then 'outpatient'
      when left(trim(bill_type_code),2) = '89' then 'other'
      when trim(place_of_service_code) = '01' then 'other'
      when trim(place_of_service_code) = '02' then 'telehealth'
      when trim(place_of_service_code) = '03' then 'other'
      when trim(place_of_service_code) = '04' then 'other'
      when trim(place_of_service_code) = '05' then 'other'
      when trim(place_of_service_code) = '06' then 'other'
      when trim(place_of_service_code) = '07' then 'other'
      when trim(place_of_service_code) = '08' then 'other'
      when trim(place_of_service_code) = '09' then 'other'
      when trim(place_of_service_code) = '10' then 'home health'
      when trim(place_of_service_code) = '11' then 'office visit'
      when trim(place_of_service_code) = '12' then 'home health'
      when trim(place_of_service_code) = '13' then 'other'
      when trim(place_of_service_code) = '14' then 'other'
      when trim(place_of_service_code) = '15' then 'other'
      when trim(place_of_service_code) = '16' then 'other'
      when trim(place_of_service_code) = '17' then 'office visit'
      when trim(place_of_service_code) = '18' then 'other'
      when trim(place_of_service_code) = '19' then 'outpatient'
      when trim(place_of_service_code) = '20' then 'urgent care'
      when trim(place_of_service_code) = '21' then 'acute inpatient'
      when trim(place_of_service_code) = '22' then 'outpatient'
      when trim(place_of_service_code) = '23' then 'emergency department'
      when trim(place_of_service_code) = '24' then 'ambulatory surgical center'
      when trim(place_of_service_code) = '25' then 'outpatient'
      when trim(place_of_service_code) = '26' then 'other'
      when trim(place_of_service_code) = '31' then 'skilled nursing facility'
      when trim(place_of_service_code) = '32' then 'skilled nursing facility'
      when trim(place_of_service_code) = '33' then 'other'
      when trim(place_of_service_code) = '34' then 'hospice'
      when trim(place_of_service_code) = '41' then 'other'
      when trim(place_of_service_code) = '42' then 'other'
      when trim(place_of_service_code) = '49' then 'office visit'
      when trim(place_of_service_code) = '50' then 'office visit'
      when trim(place_of_service_code) = '51' then 'inpatient psychiatric'
      when trim(place_of_service_code) = '52' then 'inpatient psychiatric'
      when trim(place_of_service_code) = '53' then 'mental health center'
      when trim(place_of_service_code) = '54' then 'other'
      when trim(place_of_service_code) = '55' then 'substance abuse treatment facility'
      when trim(place_of_service_code) = '56' then 'substance abuse treatment facility'
      when trim(place_of_service_code) = '57' then 'substance abuse treatment facility'
      when trim(place_of_service_code) = '58' then 'substance abuse treatment facility'
      when trim(place_of_service_code) = '60' then 'other'
      when trim(place_of_service_code) = '61' then 'inpatient rehabilitation'
      when trim(place_of_service_code) = '62' then 'outpatient rehabilitation'
      when trim(place_of_service_code) = '65' then 'dialysis center'
      when trim(place_of_service_code) = '71' then 'office visit'
      when trim(place_of_service_code) = '72' then 'office visit'
      when trim(place_of_service_code) = '81' then 'other'
      when trim(place_of_service_code) = '99' then 'other'
            else 'unmapped'
    end as encounter_type
    ,med.claim_type
    ,med.claim_id
    ,med.claim_line_number
    ,med.patient_id
    ,med.claim_start_date
    ,med.claim_end_date
    ,med.discharge_disposition_code
    ,med.billing_npi
    ,med.facility_npi
    ,med.paid_amount
    ,med.charge_amount
    ,med.bill_type_code
    ,med.revenue_center_code
    ,med.place_of_service_code
  from {{ var('medical_claim')}} med
  inner join claim_header head
    on med.claim_id = head.claim_id
    and med.claim_line_number = head.claim_line_number
) 
  
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
  ,cast(row_number() over (partition by patient_id, encounter_type, claim_type order by claim_start_date, claim_end_date) as int) as row_sequence
from encounter_type_mapping