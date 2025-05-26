select distinct account_Id -- ID аккаунта
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` billingPackages 
    where paid_At is not null
        -- Таблица аккантов с оплаченными подписками