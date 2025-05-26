select  account_Id ,                                                   -- ID аккаунта
    string_agg(distinct concat(type, ' - ', tariff)) as type_and_tariff -- Каналы и тарифы
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` billingPackages 
    where paid_At is not null and type!='equipment'
    group by 1
        -- Таблица аккаунтов и их каналов с тарифами