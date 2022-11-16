{% test inst_facility_npi(model, column_name) %}
{{ config(enabled=var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))) }}
/**  Returns rows if institutional claims have a null facility npi  **/

select {{ column_name }}
from {{ model }}
where claim_type = 'institutional'
and nullif(facility_npi,'') is null

{% endtest %}