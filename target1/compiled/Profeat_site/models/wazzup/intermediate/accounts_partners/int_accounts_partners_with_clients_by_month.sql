with partner_info as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_type_and_partner_change_with_partner_type_deduplicated`
),

months as (
    select * from `dwh-wazzup`.`analytics_tech`.`months`
)
    -- Какие аккаунты прикреплены к партнеру в этом месяце. Важно: точные данные у нас появились только с февраля 2023
select partner_id,  -- аккаунт партнера
account_id,         -- аккаунт дочки
months.month,       -- рассматриваемый месяц
partner_type,       -- тип аккаунта партнера. Важно! Берется тип партнера на рассматирваемый месяц
account_type        -- тип аккаунта дочки. Важно! Берется тип партнера на рассматриваемый месяц
from  partner_info
    inner join  months
on months.month>=date_trunc(partner_info.start_date, month) and months.month<=date_trunc(partner_info.end_date, month)