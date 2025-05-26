with registration_data as (
    select * from  `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_all_info_that_used_as_filters` 
),
int_cmuserid_edited_2_blocks as (
    select * from  `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_edited_2_blocks`
),

stg_payments_success_and_recurring as (
    select cmuserid,
            datetime,
            first_payment_datetime
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_payments_success_and_recurring`
)
,stg_cmuserid_copied_template_link as ( 
    select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_cmuserid_copied_template_link`
),

int_cmuserId__count_visitkas_visitors as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserId__count_visitkas_visitors`
),
int_cmuserid__all_visitkas_visitors as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_visitkas_visitors_with_visit_time_all_visits`
),
int_payments_promo_tarif_disctinct_cmuserid as (
    select * from  `dwh-wazzup`.`dbt_nbespalov`.`int_payments_promo_tarif_disctinct_cmuserid`
 ),


int_funnel_key_events__finding_all_users_stages as (
    select distinct
    registration_data.cmuserid succreg,
    registration_data.registration_date,
    registration_data.utm_source,
    registration_data.utm_campaign,
    registration_data.utm_medium,
    registration_data.abtest_name,
    registration_data.abtest_group,
    registration_data.abgroup_count_filter,
    registration_data.initrefferer,
    utm_traffic,
    devicetypes,
    registration_data.template_link,
    business_spheres_filter,
    eventgroupname_description,
    business_spheres_filter_description,
    os,
    case when includeinmetrics = 'yes' then registration_data.cmuserid end as templateusage,
    int_cmuserid_edited_2_blocks.cmuserid as edits,
    int_cmuserId__count_visitkas_visitors.cmuserid as activation,
    coalesce(stg_cmuserid_copied_template_link.cmuserid, int_cmuserid__all_visitkas_visitors.cmuserid) as copies,
    int_payments_promo_tarif_disctinct_cmuserid.cmuserid as posted,
    case when stg_payments_success_and_recurring.cmuserid is not null  then registration_data.cmuserId end as paid,
    case when stg_payments_success_and_recurring.cmuserid is not null and stg_payments_success_and_recurring.datetime = stg_payments_success_and_recurring.first_payment_datetime then stg_payments_success_and_recurring.cmuserid end as  first_paid,
    case when stg_payments_success_and_recurring.cmuserid is not null and stg_payments_success_and_recurring.datetime != stg_payments_success_and_recurring.first_payment_datetime then stg_payments_success_and_recurring.cmuserid end as  repeat_paid
    from registration_data
    left join int_cmuserid_edited_2_blocks on registration_data.cmuserid = int_cmuserid_edited_2_blocks.cmuserid
    left join int_cmuserid__all_visitkas_visitors on registration_data.cmuserid = int_cmuserid__all_visitkas_visitors.cmuserId
    left join stg_cmuserid_copied_template_link on registration_data.cmuserid = stg_cmuserid_copied_template_link.cmuserid
    left join int_cmuserId__count_visitkas_visitors on registration_data.cmuserid = int_cmuserId__count_visitkas_visitors.cmuserid
    left join int_payments_promo_tarif_disctinct_cmuserid on registration_data.cmuserid = int_payments_promo_tarif_disctinct_cmuserid.cmuserid
    left join stg_payments_success_and_recurring on registration_data.cmuserid = stg_payments_success_and_recurring.cmuserid
    )
select * from int_funnel_key_events__finding_all_users_stages