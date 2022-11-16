{% test inst_bill_type_code(model, column_name) %}
{{ config(enabled=var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))) }}
/**  Returns rows if institutional claims have a null bill type code  **/

select {{ column_name }}
from {{ model }}
where claim_type = 'institutional'
and nullif(bill_type_code,'') is null

{% endtest %}