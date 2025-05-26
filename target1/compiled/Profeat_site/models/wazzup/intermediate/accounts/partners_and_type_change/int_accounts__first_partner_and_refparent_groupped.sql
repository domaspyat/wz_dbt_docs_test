with first_partner_and_refparentid_groupped as (            -- Таблица с первыми партнерами, реф.папами и датой окончания связи с ними
    select 
    (case when first_value_partner_id is null then 0
    else first_value_partner_id                         
    end) partner_id,                                        -- Первый аккаунт партнера, который был у аккаунта. Если партнера не было (null), то присваеваем значение 0
    (case when first_value_refparent_id is null then 0
    else first_value_refparent_id
    end) refparent_id,                                      -- Первый аккаунт реф.папы, который был у аккаунта. Если партнера не было (null), то присваеваем значение 0
    account_id,                                             -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts
    first_value_occured_at as end_occured_at                -- Дата и время первого изменения партнера у аккаунта, то есть завершение действия первого партнера/реф.папы
    
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts_events__partner_change`
    group by 1,2,3,4)                                       -- Группировка значений, чтобы осталось по одному значению для каждого аккаунта

select * from first_partner_and_refparentid_groupped