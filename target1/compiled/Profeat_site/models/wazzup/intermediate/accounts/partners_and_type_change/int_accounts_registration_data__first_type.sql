-- Таблица аккаунтов с датой регистрации, первым типом аккаунта и датой окончания первого типа аккаунта

with accounts as (                  -- Тянем все данные из stg_accounts
    select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts`
),

int_accounts__first_type as (       -- Тянем все данные из int_accounts__first_type
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts__first_type`
),

registration_data_for_types as (                  -- Таблица аккаунтов с датой регистрации, первым типом аккаунта и датой окончания первого типа аккаунта         
    select accounts.account_id as account_id,           -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts
    accounts.register_at as start_occured_at,           -- Дата и время регистрации аккаунта, начало действия первого типа
    coalesce(int_accounts__first_type.first_value_type, accounts.type) as type,         -- Тип первого измениния, если null, то тип аккаунта в текущий момент времени
    coalesce(datetime(end_occured_at), datetime(current_timestamp,'Europe/Moscow')) as end_occured_at       -- Дата и время первого изменения типа аккаунта, если null, то текущее время
    from accounts left join int_accounts__first_type 
    on accounts.account_id=int_accounts__first_type.account_id
    )

select * from registration_data_for_types