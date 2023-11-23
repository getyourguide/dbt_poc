{% macro is_supplier_register_action() %}
    (
			(event_name = '{{var("EVENT_UICLICK")}}' AND ui.target = '{{var("TARGET_SUPPLIERREGISTERACTION")}}')

			OR (event_name = '{{var("EVENT_SUPPLIERWEBUICLICK")}}' AND ui.target = '{{var("TARGET_SUPPLIERREGISTERACTION")}}')

	)
{% endmacro %}