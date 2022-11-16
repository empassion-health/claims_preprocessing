-------------------------------------------------------------------------------
-- Author       Thu Xuan Vu
-- Created      Noveber 2022
-- Purpose      Medication list
-- Notes
-------------------------------------------------------------------------------
-- Modification History
-------------------------------------------------------------------------------
{{ config(enabled=var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))) }}

select
    cast(claim_id as varchar) as claim_id
    , cast(claim_line_number as varchar) as claim_line_number
    , cast(patient_id as varchar) as patient_id
    , cast(member_id as varchar) as member_id
    , cast(prescribing_provider_npi as varchar) as prescribing_provider_npi
    , cast(dispensing_provider_npi as varchar) as dispensing_provider_npi
    , cast(dispensing_date as varchar) as dispensing_date
    , cast(ndc_code as varchar) as ndc_code
    , cast(null as varchar) as ndc_description
    , cast(quantity as varchar) as quantity
    , cast(days_supply as varchar) as days_supply
    , cast(refills as varchar) as refills
    , cast(paid_date as varchar) as paid_date
    , cast(paid_amount as varchar) as paid_amount
    , cast(allowed_amount as varchar) as allowed_amount
    , cast(data_source as varchar) as data_source
from {{ var('pharmacy_claim')}} m