WITH profile_info AS (
    SELECT account_id, currency, type, is_employee
    FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
    WHERE is_employee IS FALSE
),
channels AS (
    SELECT *
    FROM `dwh-wazzup`.`dbt_nbespalov`.`int_channels_agg_with_trials`
),
subscriptions AS (
    SELECT
        subscription_id,
        subscription_start,
        subscription_end
    FROM `dwh-wazzup`.`dbt_nbespalov`.`int_subscription_deduplicated_by_id_combined_intervals`
),
active_channels AS (
    SELECT
        p.currency,
        p.type,
        ach.channel_id,
        ach.transport,
        ach.package_id,
        ach.min_datetime,
        ach.max_datetime,
        ach.is_free,
        ach.account_id,
        ach.whatsap_trial,
        ach.instagram_trial,
        ach.tgapi_trial,
        ach.wapi_trial,
        ach.avito_trial,
        ach.vk_trial,
        ach.telegram_trial,
        day  AS date,
        s.subscription_id,
        s.subscription_start,
        s.subscription_end
    FROM channels ach,
    UNNEST(GENERATE_DATE_ARRAY(DATE(ach.min_datetime), DATE(ach.max_datetime))) AS day
    JOIN profile_info p
        ON ach.account_id = p.account_id
    LEFT JOIN subscriptions s
        ON s.subscription_id = ach.package_id
        AND s.subscription_start <= day
        AND s.subscription_end >= day
    WHERE day BETWEEN date(ach.min_datetime) AND date(ach.max_datetime)
),partner_info AS ( 
    SELECT *
    FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_type_and_partner_change_with_partner_type_deduplicated`
),active_channels_with_min_date AS (
    SELECT
        ac.*,
        ap.account_type,
        ap.partner_type,
        ap.partner_id,
        ap.refparent_id,
      --  MIN(ac.date) OVER (PARTITION BY ac.account_id, ac.transport) AS min_channel_date_by_transport,
      --  MIN(ac.date) OVER (PARTITION BY ac.account_id) AS min_channel_date_by_account
    FROM active_channels ac
    LEFT JOIN partner_info ap
        ON ac.account_id = ap.account_id
        AND ac.date BETWEEN ap.start_date AND ap.end_date
    WHERE ac.channel_id NOT IN ('4f764c42-2372-4233-b3dc-891df42e88a5')
)
select *
from active_channels_with_min_date
where date >= '2022-01-01'