with left_and_returned_accounts as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`mart_left_and_returned_accounts_with_partner_and_account_type_with_dates`
),

segments_by_date as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`mart_active_accounts_by_days_by_segment`
)
    -- Периоды активности активной оплаченной подписки у пользователей с определением сегментов по дням
select left_and_returned_accounts.*, 
segments_by_date.segment,   -- Сегмент
russian_country_name,       -- Название страны на русском языке
region_international        -- Регион
from left_and_returned_accounts 
left join segments_by_date 
on segments_by_date.date=left_and_returned_accounts.date 
and segments_by_date.account_id=left_and_returned_accounts.account_id
left join `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info 
on profile_info.account_id=left_and_returned_accounts.account_id