with channels as (
    select account_id, 
    created_at, 
    created_date,
    transport,
    row_number() over (partition by account_id order by created_at asc) rn
    from `dwh-wazzup`.`dbt_nbespalov`.`int_channels__not_temporary`),

first_channel as (
    select account_id,  -- ID аккаунта
    created_date,       -- Дата создания канала
    transport           -- Транспорт канала
    from channels
    where rn=1
)
    -- Таблица с первыми добавленными каналами на аккаунт
select * from first_channel