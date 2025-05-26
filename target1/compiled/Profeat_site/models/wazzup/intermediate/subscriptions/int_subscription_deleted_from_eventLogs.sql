
with deleted_at_to_deduplicate as (
        select 
        subject_id as subscription_id,
        occured_at as deleted_at,
        row_number() over (partition by subject_id order by occured_at asc) rn
        from `dwh-wazzup`.`dbt_nbespalov`.`stg_eventLogs` 
        where log_type='billingPackages'
        and state='deleted'
        and id not in (55215321,56146810,17248256,55229499,48304241,
        69071521,25119641,53994778,54982890,56180437,54194487,54937180,
        53994776) 
        /*подписки, которых восстанавливали после удаления. Их очень мало и с 2022 года такого не было, поэтому вставила плейнтекстом, а не запросом 
        проверочный запрос в postgresql 
        with eventLogs_data as (select 
                    (case when "details"->>'state'='deleted' then 'deleted'
                    when "details"->>'state'='active' then 'active'
                    end
                    ) as state, 
                    "subjectId",
                    lag("details"->>'state',1) over (partition by "subjectId" order by "id" desc) as previous_state ,
                    id
                    from "eventLogs" 
                    where "details"->>'state' in ('deleted','active')
                    )

                    select previous_state,
                    state,
                    "subjectId",
                    id
                    from eventLogs_data 
                    where previous_state='active' and state='deleted'
        */
        )
select subscription_id,                        -- ID подписки
deleted_at from  deleted_at_to_deduplicate     -- Дата и время удаления
where rn=1