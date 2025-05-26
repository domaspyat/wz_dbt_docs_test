SELECT profile_info.*, 
stg_billingpackages.period,                     -- Текущая длительность подписки в месяцах
stg_billingpackages.quantity,                   -- Текущее количество каналов в подписке. Но, для tech-partner-postpay - 10000. Это было сделано сознательно, для ,чтобы все каналы-дочки tech-partner-postpay были в одной подписке
stg_billingpackages.tariff,                     -- Текущий тариф подписки
stg_billingpackages.guid,                       -- Идентификатор подписки.Генерируется Postgress при создании записи
stg_billingpackages.type as subscription_type,  -- Транспорт подписки
stg_billingPackages.expires_at                  -- Текущая дата окончания подписки
 from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info
inner join `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` stg_billingpackages on stg_billingpackages.account_id=profile_info.account_id
where paid_at is not null and stg_billingpackages.state='active'  and not is_employee
    -- Таблица с информацией по текущим оплаченным активным подпискам пользователей