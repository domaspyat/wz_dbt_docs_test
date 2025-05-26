with billing_data_cleared as

 (select * from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_old_billing_with_who_paid`)
,
billing_data_cleared_with_rn as
    (select account_id,                 -- ID аккаунта
            guid,                       -- guid платежа
            partner_account_id,         -- ID аккаунта партнера, если он плательщик
            row_number() over (partition by account_id order by paid_at desc) as rn_number_billing 
     from billing_data_cleared
),

billing_data_cleared_last_row as  (
    select * 
    from billing_data_cleared_with_rn
    where rn_number_billing=1           -- Берутся последние оплаты
    )
    -- Таблица с аккаунтами и последним гуидом платежа
select * from billing_data_cleared_last_row