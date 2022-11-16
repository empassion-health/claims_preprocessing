{% test inst_discharge_disposition_code(model, column_name) %}
{{ config(enabled=var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))) }}
/**  Returns rows if institutional claims have a null discharge disposition code  **/

select {{ column_name }}
from {{ model }}
where claim_type = 'institutional'
and nullif(discharge_disposition_code,'') is null

{% endtest %}