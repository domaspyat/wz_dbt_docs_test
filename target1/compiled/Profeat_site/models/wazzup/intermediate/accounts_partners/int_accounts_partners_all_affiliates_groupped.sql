select partner_id,  -- аккаунт партнера
account_id,         -- аккаунт дочки
min(start_date) as created_date,    -- дата прикрепления дочки
max(end_date) as max_end_date       -- дата открепления дочки (равна сегодняшнему дню, если дочка все еще прикреплена к партнеру)
from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_type_and_partner_change_with_partner_type_deduplicated`
where partner_id is not null
group by 1,2
    -- Показывает, когда к партнерам были прикреплены дочки