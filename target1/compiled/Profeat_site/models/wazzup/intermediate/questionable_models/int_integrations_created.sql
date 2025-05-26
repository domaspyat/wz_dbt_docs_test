select  integration_id,
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
        cast((case 
            when state='active' then CURRENT_TIMESTAMP()
            when deleted_at is not null then deleted_at
            when lag( created_at  ,1) over (partition by account_id order by created_at  DESC) is null
                then  CURRENT_TIMESTAMP()
            else lag( cast(created_at as TIMESTAMP)  ,1) over (partition by account_id order by created_at  DESC) 
            end) as date) as integration_end_date,
        (case 
        when integration_type in ('api_v2','api_v3') then web_hooks_url
        when integration_type='api_v1' then domain
        end)
        as api_type_field
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_integrations`