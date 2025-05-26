with mart_revenue_by_segments as (
        select * from `dwh-wazzup`.`dbt_nbespalov`.`mart_revenue_by_segments_aggregated`
), 
    mart_left_and_returned_accounts_with_partner_and_account_type_with_dates_and_lt_info_aggregated as (
        select * from `dwh-wazzup`.`dbt_nbespalov`.`mart_left_and_returned_accounts_with_partner_and_account_type_with_dates_and_lt_info_aggregated`
), 
    mart_left_and_returned_accounts_with_partner_and_account_type_with_dates_and_ltv_info_aggregated as (
        select * from `dwh-wazzup`.`dbt_nbespalov`.`mart_left_and_returned_accounts_with_partner_and_account_type_with_dates_and_ltv_info_aggregated`
), 
    mart_c1_aggregated as (
        select * from `dwh-wazzup`.`dbt_nbespalov`.`mart_c1_aggregated`
),  
    mart_active_accounts_by_month_by_segment_aggregated_active_at_the_end_of_the_month as (
        select * from `dwh-wazzup`.`dbt_nbespalov`.`mart_active_accounts_by_month_by_segment_aggregated_active_at_the_end_of_the_month`
),
    mart_accounts_registration_sources_aggregated as (
        select * from `dwh-wazzup`.`dbt_nbespalov`.`mart_accounts_registration_sources_aggregated`
),
    int_channels_active_paid_monthly_aggregated as (
        select * from `dwh-wazzup`.`dbt_nbespalov`.`int_channels_active_paid_monthly_aggregated`
),
    int_accounts_partners_metrics_for_partners_active_users_aggregated as (
        select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partners_metrics_for_partners_active_users_aggregated`
),mart_retention_of_second_month as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`mart_retention_of_second_month`
)   -- Все метрики для рекламного отчета
  select commercial_report.*,
          mart_revenue_by_segments.*except(segments_aggregated_,client_type_,paid_month,market_type,registration_source_agg_),
          mart_left_and_returned_accounts_with_partner_and_account_type_with_dates_and_lt_info_aggregated.*except(segments_aggregated_,client_type_,month,market_type,registration_source_agg_),
          mart_left_and_returned_accounts_with_partner_and_account_type_with_dates_and_ltv_info_aggregated.*except(segments_aggregated_,client_type_,month,market_type,registration_source_agg_),
          mart_c1_aggregated.*except(segments_aggregated_,client_type_,registration_month,market_type,registration_source_agg_),
          mart_active_accounts_by_month_by_segment_aggregated_active_at_the_end_of_the_month.*except(segments_aggregated_,client_type_,month,market_type,registration_source_agg_),
          mart_accounts_registration_sources_aggregated.*except(segments_aggregated_,client_type_,registration_month,market_type,registration_source_agg_),
          int_channels_active_paid_monthly_aggregated.*except(segments_aggregated_,client_type_,month,market_type,registration_source_agg_),
          int_accounts_partners_metrics_for_partners_active_users_aggregated.*except(segments_aggregated_,client_type_,month,market_type,registration_source_agg_),
          mart_retention_of_second_month.*except(segments_aggregated_,client_type_,month,market_type,registration_source_agg_)
  from `dwh-wazzup`.`analytics_tech`.`metrics_template_for_commercial_director_report` commercial_report
  left join mart_revenue_by_segments
                                    on commercial_report.segments_aggregated_ = mart_revenue_by_segments.segments_aggregated_
                                       and commercial_report.client_type_ = mart_revenue_by_segments.client_type_
                                       and commercial_report.month = mart_revenue_by_segments.paid_month
                                       and commercial_report.market_type = mart_revenue_by_segments.market_type
                                       and commercial_report.registration_source_agg_ = mart_revenue_by_segments.registration_source_agg_

  left join mart_left_and_returned_accounts_with_partner_and_account_type_with_dates_and_lt_info_aggregated 
                                    on commercial_report.segments_aggregated_ = mart_left_and_returned_accounts_with_partner_and_account_type_with_dates_and_lt_info_aggregated.segments_aggregated_
                                       and commercial_report.client_type_ = mart_left_and_returned_accounts_with_partner_and_account_type_with_dates_and_lt_info_aggregated.client_type_
                                       and commercial_report.month = mart_left_and_returned_accounts_with_partner_and_account_type_with_dates_and_lt_info_aggregated.month
                                       and commercial_report.market_type = mart_left_and_returned_accounts_with_partner_and_account_type_with_dates_and_lt_info_aggregated.market_type
                                       and commercial_report.registration_source_agg_ = mart_left_and_returned_accounts_with_partner_and_account_type_with_dates_and_lt_info_aggregated.registration_source_agg_

  left join mart_left_and_returned_accounts_with_partner_and_account_type_with_dates_and_ltv_info_aggregated 
                                    on commercial_report.segments_aggregated_ = mart_left_and_returned_accounts_with_partner_and_account_type_with_dates_and_ltv_info_aggregated.segments_aggregated_
                                       and commercial_report.client_type_ = mart_left_and_returned_accounts_with_partner_and_account_type_with_dates_and_ltv_info_aggregated.client_type_
                                       and commercial_report.month = mart_left_and_returned_accounts_with_partner_and_account_type_with_dates_and_ltv_info_aggregated.month
                                       and commercial_report.market_type = mart_left_and_returned_accounts_with_partner_and_account_type_with_dates_and_ltv_info_aggregated.market_type
                                       and commercial_report.registration_source_agg_ = mart_left_and_returned_accounts_with_partner_and_account_type_with_dates_and_ltv_info_aggregated.registration_source_agg_

  left join  mart_c1_aggregated 
                                    on  commercial_report.segments_aggregated_ = mart_c1_aggregated.segments_aggregated_
                                       and commercial_report.client_type_ = mart_c1_aggregated.client_type_
                                       and commercial_report.month = mart_c1_aggregated.registration_month
                                       and commercial_report.market_type = mart_c1_aggregated.market_type
                                       and commercial_report.registration_source_agg_ = mart_c1_aggregated.registration_source_agg_

  left join mart_active_accounts_by_month_by_segment_aggregated_active_at_the_end_of_the_month 
                                                                    on commercial_report.segments_aggregated_ = mart_active_accounts_by_month_by_segment_aggregated_active_at_the_end_of_the_month.segments_aggregated_
                                                                        and commercial_report.client_type_ = mart_active_accounts_by_month_by_segment_aggregated_active_at_the_end_of_the_month.client_type_
                                                                        and commercial_report.month = mart_active_accounts_by_month_by_segment_aggregated_active_at_the_end_of_the_month.month
                                                                        and commercial_report.market_type = mart_active_accounts_by_month_by_segment_aggregated_active_at_the_end_of_the_month.market_type
                                                                        and commercial_report.registration_source_agg_ = mart_active_accounts_by_month_by_segment_aggregated_active_at_the_end_of_the_month.registration_source_agg_


  left join mart_accounts_registration_sources_aggregated
                                                                    on commercial_report.segments_aggregated_ = mart_accounts_registration_sources_aggregated.segments_aggregated_
                                                                        and commercial_report.client_type_ = mart_accounts_registration_sources_aggregated.client_type_
                                                                        and commercial_report.month = mart_accounts_registration_sources_aggregated.registration_month
                                                                        and commercial_report.market_type = mart_accounts_registration_sources_aggregated.market_type 
                                                                        and commercial_report.registration_source_agg_ = mart_accounts_registration_sources_aggregated.registration_source_agg_

  left join int_channels_active_paid_monthly_aggregated
                                                                    on commercial_report.segments_aggregated_ = int_channels_active_paid_monthly_aggregated.segments_aggregated_
                                                                        and commercial_report.client_type_ = int_channels_active_paid_monthly_aggregated.client_type_
                                                                        and commercial_report.month = int_channels_active_paid_monthly_aggregated.month
                                                                        and commercial_report.market_type = int_channels_active_paid_monthly_aggregated.market_type 
                                                                        and commercial_report.registration_source_agg_ = int_channels_active_paid_monthly_aggregated.registration_source_agg_ 

  left join int_accounts_partners_metrics_for_partners_active_users_aggregated
                                                                    on commercial_report.segments_aggregated_ = int_accounts_partners_metrics_for_partners_active_users_aggregated.segments_aggregated_
                                                                        and commercial_report.client_type_ = int_accounts_partners_metrics_for_partners_active_users_aggregated.client_type_
                                                                        and commercial_report.month = int_accounts_partners_metrics_for_partners_active_users_aggregated.month
                                                                        and commercial_report.market_type = int_accounts_partners_metrics_for_partners_active_users_aggregated.market_type 
                                                                        and commercial_report.registration_source_agg_ = int_accounts_partners_metrics_for_partners_active_users_aggregated.registration_source_agg_ 

 left join mart_retention_of_second_month
                                                                    on commercial_report.segments_aggregated_ = mart_retention_of_second_month.segments_aggregated_
                                                                        and commercial_report.client_type_ = mart_retention_of_second_month.client_type_
                                                                        and commercial_report.month = mart_retention_of_second_month.month
                                                                        and commercial_report.market_type = mart_retention_of_second_month.market_type 
                                                                        and commercial_report.registration_source_agg_ = mart_retention_of_second_month.registration_source_agg_


  where commercial_report.month <= date_trunc(current_date(),month)