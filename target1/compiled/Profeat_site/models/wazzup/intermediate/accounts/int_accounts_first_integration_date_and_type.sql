with integrations_data as (
    select account_id, 
    first_value((case when is_integration_not_valid then null else integration_type end) ignore nulls) over (partition by account_id order by created_at asc rows between unbounded preceding and unbounded following) as 
    integration_type_valid,
    first_value(integration_type) over (partition by account_id order by created_at asc rows between unbounded preceding and unbounded following) as integration_type,
    first_value((case when is_integration_not_valid then null else created_date end) ignore nulls) over (partition by account_id order by created_at asc rows between unbounded preceding and unbounded following) as 
    integration_type_valid_created_date,
    first_value(created_date) over (partition by account_id order by created_at asc rows between unbounded preceding and unbounded following) as 
    created_date  from `dwh-wazzup`.`dbt_nbespalov`.`int_integrations_created_with_pipedrive`
),

first_integration as ( 
    select account_id,                      -- ID аккаунта
    integration_type,                       -- Тип интеграции
    integration_type_valid,                 -- ТИп валидной интеграции
    integration_type_valid_created_date,    -- Дата создания валидной интеграции
    created_date                            -- Дата создания интеграции
    from integrations_data
    group by 1,2,3,4,5)
    -- Таблица с первыми интеграциями, добавленными на аккаунт
select * from first_integration