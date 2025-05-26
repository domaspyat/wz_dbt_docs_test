with integrations as (
    select integration_id,
        account_id,
        integration_type,
        state,
        domain,
        disabled_to,
        created_at,
        created_date,
        deleted_at,
        deleted_date, 
        activated_at,
        activated_date,
        crm_name,
        web_hooks_url,
        marketplace_type,
        _ibk,
        (case 
        when integration_type in ('api_v2','api_v3') then web_hooks_url
        when integration_type='api_v1' then domain
        end)
        as api_type_field
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_integrations`
),

pipedrive_integrations as (
     select id as integration_id,                   -- ID интеграции
        account_id,                                 -- ID аккаунта
        'pipedrive' as integration_type,            -- Тип интеграции
        status as state,                            -- Состояние интеграции
        domain,                                     -- Домен интеграции
        cast(null as timestamp) as disabled_to,     -- Если интеграция в state 'paused', тут будет время, в которое её нужно вернуть в активное состояние
        cast(created_at as datetime) as created_at, -- Дата и время создания интеграции
        cast( created_at as date) as created_date,  -- Дата создания интеграции
        deleted_at,                                 -- Дата и время удаления интеграции юзером
        cast(deleted_at as date) as deleted_date,   -- Дата удаления интеграции юзером
        cast(null as TIMESTAMP) as activated_at,    -- Используется в интеграциях с amo. Дата и время когда активировали интеграцию
        cast(null as date) as activated_date,       -- Используется в интеграциях с amo. Дата когда активировали интеграцию
        cast(null as string) as crm_name,           -- Название CRM. Пишется при установке с маркетплейса интеграций
        cast(null as string) as web_hooks_url,      -- Адрес для вебхуков для интеграций api_v2
        cast(null as string) as marketplace_type,   -- Тип api_v3 интеграции из маркетплейса
        cast( created_at as date) as _ibk,          -- Дата создания интеграции. Необходимо для партицирования данных в BigQuery
         cast(null as string) as api_type_field     -- web_hooks_url в случае api_v2 и api_v3. domain в случае api_v1
    from `dwh-wazzup`.`wazzup`.`pipedrive_integrations`
),

integrations_with_pipedrive as (
    select * from integrations
    union all
    select * from pipedrive_integrations
)


select  -- Таблица созданных интеграций и информации по ним, учитывая pipedrive
        *, 
        (case when  (created_date=deleted_date)   
        or (integration_type like '%api%' and crm_name is null and web_hooks_url is null and marketplace_type is null and api_type_field is null) 
        then True else False end) as is_integration_not_valid,  --не все установки интеграции одинаково полезны: если они были удалены в тот же день или по ним нет никакой интеграции (в случае api_v3), то мы их не считаем

        
        cast((case 
            when state='active' then CURRENT_TIMESTAMP()
            when deleted_at is not null then deleted_at
            when lag( created_at  ,1) over (partition by account_id order by created_at  DESC) is null
                then  CURRENT_TIMESTAMP()
            else lag( cast(created_at as TIMESTAMP)  ,1) over (partition by account_id order by created_at  DESC) 
            end) as date) as integration_end_date -- Дата окончания действия интеграции
            from integrations_with_pipedrive