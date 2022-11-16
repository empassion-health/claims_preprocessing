{% test prof_place_of_service_code(model, column_name) %}
{{ config(enabled=var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))) }}
/**  Returns rows if profession claims have a null place of service  **/

select {{ column_name }}
from {{ model }}
where claim_type = 'professional'
and nullif(place_of_service_code,'') is null

{% endtest %}