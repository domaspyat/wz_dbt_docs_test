with registration_data as (
    select * from  `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_attribution_devices_phone_payment_template` 
),

int_cmuserid_edits as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_edited_all`
)
,
int_cmuserid_business_spheres as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_business_spheres` 
),

stg_cmuserid_copied_template_link as ( 
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_copy_and_activation_combined_time`
),

int_cmuserId__count_visitkas_visitors_one_client as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserId__count_visitkas_visitors_one_client`
),

int_cmuserId__count_visitkas_visitors as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserId__count_visitkas_visitors`
),

int_cmuserId__count_visitkas_visitors_five_clients as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserId__count_visitkas_visitors_five_clients`
),

int_cmuserId__count_visitkas_visitors_ten_clients as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserId__count_visitkas_visitors_ten_clients`
),
int_payments_promo_tarif_disctinct_cmuserid as (
    select * from  `dwh-wazzup`.`dbt_nbespalov`.`int_payments_promo_tarif_disctinct_cmuserid`
 ),


int_funnel_key_events__finding_all_users_stages as (
select distinct
    registration_data.cmuserid,
    registration_data.registration_date,
    registration_data.registration_datetime,
    registration_data.utm_source,
    registration_data.utm_campaign,
    registration_data.utm_medium,
    os,
    registration_data.initreferrer as initrefferer,
    utm_traffic,
    registration_data.device as devicetypes,
    registration_data.template_link,
    business_spheres_filter,
    eventgroupname_description,
    business_spheres_filter_description,
    case when includeinmetrics = 'yes' then int_cmuserid_business_spheres.business_select_datetime end as templateusage,
    int_cmuserid_edits.edits_datetime_2_blocks as edits,
    int_cmuserid_edits.edits_datetime_1_block as edits_one_block,
    int_cmuserid_edits.edits_datetime_3_blocks as edits_three_blocks,
    int_cmuserId__count_visitkas_visitors.activation_datetime as activation,
    int_cmuserId__count_visitkas_visitors_one_client.activation_datetime as activation_one_client,
    int_cmuserId__count_visitkas_visitors_five_clients.activation_datetime as activation_five_clients,
    int_cmuserId__count_visitkas_visitors_ten_clients.activation_datetime as activation_ten_clients,
    stg_cmuserid_copied_template_link.copied_datetime as copies,
    int_payments_promo_tarif_disctinct_cmuserid.posted_datetime as posted,
    case when stg_cmuserid_copied_template_link.copied_datetime is null then False
    else True end as has_posted,
    registration_data.first_payment_datetime as paid
    from registration_data
    left join  int_cmuserid_edits on registration_data.cmuserid = int_cmuserid_edits.cmuserid
    left join int_cmuserid_business_spheres on registration_data.cmuserid = int_cmuserid_business_spheres.cmuserid
    left join stg_cmuserid_copied_template_link on registration_data.cmuserid = stg_cmuserid_copied_template_link.cmuserid
    left join int_cmuserId__count_visitkas_visitors on registration_data.cmuserid = int_cmuserId__count_visitkas_visitors.cmuserid
    left join int_cmuserId__count_visitkas_visitors_one_client on registration_data.cmuserid = int_cmuserId__count_visitkas_visitors_one_client.cmuserid
    left join int_cmuserId__count_visitkas_visitors_five_clients on registration_data.cmuserid = int_cmuserId__count_visitkas_visitors_five_clients.cmuserid
    left join int_cmuserId__count_visitkas_visitors_ten_clients on registration_data.cmuserid = int_cmuserId__count_visitkas_visitors_ten_clients.cmuserid
    left join int_payments_promo_tarif_disctinct_cmuserid on registration_data.cmuserid = int_payments_promo_tarif_disctinct_cmuserid.cmuserid
    )
select *
from int_funnel_key_events__finding_all_users_stages