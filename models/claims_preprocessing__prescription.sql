{{ config(enabled=var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))) }}

select
    cast(claim_id as {{ dbt.type_string() }}) as claim_id
    , cast(claim_line_number as {{ dbt.type_string() }}) as claim_line_number
    , cast(patient_id as {{ dbt.type_string() }}) as patient_id
    , cast(member_id as {{ dbt.type_string() }}) as member_id
    , cast(prescribing_provider_npi as {{ dbt.type_string() }}) as prescribing_provider_npi
    , cast(dispensing_provider_npi as {{ dbt.type_string() }}) as dispensing_provider_npi
    , cast(dispensing_date as date ) as dispensing_date
    , cast(ndc_code as {{ dbt.type_string() }}) as ndc_code
    , cast(quantity as int ) as quantity
    , cast(days_supply as int ) as days_supply
    , cast(refills as int) as refills
    , cast(paid_date as date ) as paid_date
    , cast(paid_amount as numeric ) as paid_amount
    , cast(allowed_amount as numeric ) as allowed_amount
    , cast(data_source as {{ dbt.type_string() }}) as data_source
from {{ var('pharmacy_claim')}} m