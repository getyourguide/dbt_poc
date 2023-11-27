import os

# Replace these strings with their respective values
replacement_dict = {
    "{% reference 'dwh.dim_transaction_cohort' %}": "{{ source('dwh', 'dim_transaction_cohort') }}",
    "{% reference 'marketing.dim_touchpoint' %}": "{{ source('marketing', 'dim_touchpoint') }}",
    "{% reference 'marketing.fact_touchpoint_source_market' %}": "{{ source('marketing', 'fact_touchpoint_source_market') }}",
    "{% reference 'dwh.dim_location' %}": "{{ source('dwh', 'dim_location') }}",
    "{% reference 'dwh.dim_purchase_type' %}": "{{ source('dwh', 'dim_purchase_type') }}",
    "{% reference 'dwh.dim_country' %}": "{{ source('dwh', 'dim_country') }}",
    "{% reference 'dwh.fact_touchpoint' %}": "{{ ref('fact_touchpoint') }}",
    "{% reference 'dwh.fact_session' %}": "{{ ref('fact_session') }}",
    "{% reference 'dwh.dim_date' %}": "{{ source('dwh', 'dim_date') }}",
    "{% reference 'dwh.fact_session_adp' %}": "{{ ref('fact_session_adp') }}",
    "{% reference 'dwh.dim_platform' %}": "{{ source('dwh', 'dim_platform') }}",
    "{% reference 'dwh.fact_booking' %}": "{{ source('dwh', 'fact_booking') }}",
    "{% reference 'dwh.dim_device' %}": "{{ source('dwh', 'dim_device') }}",
    "{% reference 'marketplace_reports.fact_booking_trip' %}": "{{ ref('fact_booking_trip') }}",
    "{% reference 'default.events' %}": "{{ source('default', 'events') }}",
    "{% reference 'marketplace_reports.sessions_discovery' %}": "{{ ref('sessions_discovery') }}",
    "{% reference 'default.dim_date' %}": "{{ source('default', 'dim_date') }}",
    "{% reference 'default.agg_contribution_margin' %}": "{{ ref('agg_contribution_margin') }}",
    "{% reference 'dwh.dim_attribution_channel_group' %}": "{{ source('dwh', 'dim_attribution_channel_group') }}",
    "{% reference 'dwh.dim_tour' %}": "{{ source('dwh', 'dim_tour') }}",
    "{% reference 'dwh.dim_user' %}": "{{ source('dwh', 'dim_user') }}",
    "{% reference 'default.stg_contribution_margin_booking_base' %}": "{{ ref('stg_contribution_margin_booking_base') }}",
    "{% reference 'default.agg_attribution_channel_weights' %}": "{{ source('default', 'agg_attribution_channel_weights') }}",
    "{% reference 'dwh.dim_reseller' %}": "{{ source('dwh', 'dim_reseller') }}",
    "{% reference 'dwh.fact_customer_trip' %}": "{{ source('dwh', 'fact_customer_trip') }}",
    "{% reference 'reports.agg_metrics_global_extended_snapshot' %}": "{{ ref('agg_metrics_global_extended_snapshot') }}",
    "{% reference 'default.fact_attribution_channel_weights' %}": "{{ source('default', 'fact_attribution_channel_weights') }}",
    "{% reference 'dwh.dim_customer' %}": "{{ source('dwh', 'dim_customer') }}",
    "{% reference 'dwh.fact_shopping_cart' %}": "{{ source('dwh', 'fact_shopping_cart') }}",
    "{% reference 'dwh.fact_forecast' %}": "{{ source('dwh', 'fact_forecast') }}",
    "{% reference 'dwh.dim_forecast_config_abacus' %}": "{{ source('dwh', 'dim_forecast_config_abacus') }}",
    "{% reference 'reports.agg_metrics_drill_down' %}": "{{ ref('agg_metrics_drill_down') }}",
    "{% reference 'reports.agg_metrics_global_extended' %}": "{{ ref('agg_metrics_global_extended') }}",
    "{% reference 'public.dim_date_deprecated' %}": "{{ source('public', 'dim_date_deprecated') }}",
    "{% reference 'reports.agg_metrics_sales' %}": "{{ ref('agg_metrics_sales') }}",
    "{% reference 'db_mirror_dbz.catalog__tour_to_location' %}": "{{ source('db_mirror_dbz', 'catalog__tour_to_location') }}",
    "{% reference 'dwh.fact_budget' %}": "{{ source('dwh', 'fact_budget') }}",
    "{% reference 'dwh.dim_budget_config_abacus' %}": "{{ source('dwh', 'dim_budget_config_abacus') }}",
    "{% reference 'default.fact_attribution' %}": "{{ source('default', 'fact_attribution') }}",
    "{% reference 'dwh.fact_attribution_visitor_history' %}": "{{ ref('fact_attribution_visitor_history') }}",
    "{% reference 'marketing.dim_promo_with_display_type' %}": "{{ source('marketing', 'dim_promo_with_display_type') }}",
    "{% reference 'dwh.dim_display_type' %}": "{{ source('dwh', 'dim_display_type') }}",
    "{% reference 'dwh.dim_landing_page_type' %}": "{{ source('dwh', 'dim_landing_page_type') }}",
    "{% reference 'dwh.dim_cost_provider' %}": "{{ source('dwh', 'dim_cost_provider') }}",
    "{% reference 'reports.agg_metrics_drill_down_3_years_snapshot' %}": "{{ ref('agg_metrics_drill_down_3_years_snapshot') }}",
    "{% reference 'reports.agg_bookings_drill_down' %}": "{{ ref('agg_bookings_drill_down') }}",
    "{% reference 'reports.agg_customers_drill_down' %}": "{{ ref('agg_customers_drill_down') }}",
    "{% reference 'reports.agg_marketing_drill_down' %}": "{{ ref('agg_marketing_drill_down') }}",
    "{% reference 'reports.agg_metrics_contribution_margin_drill_down' %}": "{{ ref('agg_metrics_contribution_margin_drill_down') }}",
    "{% reference 'marketplace_reports.agg_discovery_session' %}": "{{ ref('agg_discovery_session') }}",
    "{% reference 'events.fact_session' %}": "{{ ref('fact_session') }}",
    "{% reference 'dwh.fact_gwc' %}": "{{ source('dwh', 'fact_gwc') }}",
    "{% reference 'dwh.fact_supplier_adjustment_payment' %}": "{{ source('dwh', 'fact_supplier_adjustment_payment') }}",
    "{% reference 'marketing.fact_booking_marketing' %}": "{{ source('marketing', 'fact_booking_marketing') }}",
    "{% reference 'dwh.fact_accounting_transaction' %}": "{{ source('dwh', 'fact_accounting_transaction') }}",
    "{% reference 'default.contribution_margin_fixed_adspend_upload' %}": "{{ source('default', 'contribution_margin_fixed_adspend_upload') }}",
    "{% reference 'default.agg_contribution_margin_breakage' %}": "{{ source('default', 'agg_contribution_margin_breakage') }}",
    "{% reference 'default.cost_allocation_breakage_upload' %}": "{{ source('default', 'cost_allocation_breakage_upload') }}",
    "{% reference 'dwh.fact_booking_cancelation_request' %}": "{{ source('dwh', 'fact_booking_cancelation_request') }}",
    "{% reference 'dwh.dim_user_history' %}": "{{ source('dwh', 'dim_user_history') }}",
    "{% reference 'dwh.fact_session_search' %}": "{{ ref('fact_session_search') }}",
    "{% reference 'reports.budget_abacus' %}": "{{ ref('budget_abacus') }}",
    "{% reference 'reports.forecast_abacus' %}": "{{ ref('forecast_abacus') }}",
    "{% reference 'reports.agg_metrics_drill_down_3_years' %}": "{{ ref('agg_metrics_drill_down_3_years') }}",
    "{% reference 'default.agg_source_market_cost_allocation' %}": "{{ ref('agg_source_market_cost_allocation') }}",
    "{% reference 'default.stg_contribution_margin_without_ad_spend' %}": "{{ ref('stg_contribution_margin_without_ad_spend') }}",
    "{% reference 'dwh.dim_destination_group' %}": "{{ source('dwh', 'dim_destination_group') }}",
    "{% reference 'reports.agg_metrics_global' %}": "{{ ref('agg_metrics_global') }}",
    "{% reference 'reports.agg_metrics_contribution_margin' %}": "{{ ref('agg_metrics_contribution_margin') }}",
    "{% reference 'reports.agg_metrics_trip_customers' %}": "{{ ref('agg_metrics_trip_customers') }}",
    "{% reference 'marketplace_reports.sessions_booking_interactions' %}": "{{ ref('sessions_booking_interactions') }}",
    "{% reference 'marketplace_reports.sessions_search_interactions' %}": "{{ ref('sessions_search_interactions') }}",
    "{% reference 'marketplace_reports.sessions_activity_interactions' %}": "{{ ref('sessions_activity_interactions') }}",
    "{% reference 'marketplace_reports.count_sessions_60_days' %}": "{{ ref('count_sessions_60_days') }}",
    "{% reference 'dwh.dim_tour_category' %}": "{{ source('dwh', 'dim_tour_category') }}",
    "{% reference 'test.events_search' %}": "{{ source('test', 'events_search') }}",
    "{% reference 'dwh.dim_reseller_campaign' %}": "{{ source('dwh', 'dim_reseller_campaign') }}",
    "{% reference 'reports.agg_metrics_events' %}": "{{ ref('agg_metrics_events') }}",
    "{% reference 'reports.agg_metrics_marketing' %}": "{{ ref('agg_metrics_marketing') }}",
    "{% config 'partition_column' 'date' %}": "",
    "{% config 'load_to_snowflake' 'false' %}": "",
    "{% if is_incremental() %}": "{% if is_incremental() %}",
    "{% config 'load_to_snowflake' 'true' %}": "",
    "{% config 'partition_column' 'check_date' %}": "",
    "{% config 'partition_column' 'snapshot_date' %}": "",
    "{% config 'snowflake_output_schema' 'public' %}": "",
    "{% config 'partition_column' 'report_date' %}": "",
    "{% if not(is_incremental()) %}": "{% if not is_incremental() %}",
    "{% if not(is_dev_mode()) && not(is_incremental()) %}": "{% if target.name != 'dev' and not is_incremental() %}",
    "{% if is_dev_mode() %}": "{% if target.name == 'dev' %}",
    "{% if is_dev_mode() && not(is_incremental()) %}": "{% if target.name == 'dev' and not is_incremental() %}",
    "{% var 'source-market-cost-allocation-start-date' %}": "{{ var 'source-market-cost-allocation-start-date' }}",
    "{% var 'cost-allocation-marketing-end-date' %}": "{{ var 'cost-allocation-marketing-end-date' }}",
    "{% var 'based-on-events-start-date' %}": "{{ var 'based-on-events-start-date' }}",
    "{% var 'end-date' %}": "{{ var 'end-date' }}",
    "{% var 'start-date' %}": "{{ var 'start-date' }}",
    "{% var 'touchpoint-start' %}": "{{ var 'touchpoint-start' }}",
    "{% var 'touchpoint-end' %}": "{{ var 'touchpoint-end' }}",
    "{{ var 'source-market-cost-allocation-start-date' }}": "{{ var ('source-market-cost-allocation-start-date') }}",
    "{{ var 'cost-allocation-marketing-end-date' }}": "{{ var ('cost-allocation-marketing-end-date') }}",
    "{{ var 'based-on-events-start-date' }}": "{{ var ('based-on-events-start-date') }}",
    "{{ var 'end-date' }}": "{{ var ('end-date') }}",
    "{{ var 'start-date' }}": "{{ var ('start-date') }}",
    "{{ var 'touchpoint-start' }}": "{{ var ('touchpoint-start') }}",
    "{{ var 'touchpoint-end' }}": "{{ var ('touchpoint-end') }}",
    # Add more replacements as needed
}

directory_path = '/Users/zaher.wanli/repos/dbt_poc/models/dwh/prio1_rivulus'

def replace_strings_in_file(file_path, replacements):
    with open(file_path, 'r') as file:
        file_content = file.read()

    for key, value in replacements.items():
        file_content = file_content.replace(key, value)

    with open(file_path, 'w') as file:
        file.write(file_content)

def process_directory(directory_path, replacements):
    for root, dirs, files in os.walk(directory_path):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            if not file_name.endswith('.py'):
                print("processing", file_path)
                replace_strings_in_file(file_path, replacements)

if __name__ == "__main__":
    process_directory(directory_path, replacement_dict)