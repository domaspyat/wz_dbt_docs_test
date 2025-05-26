with count_channels as (
    select account_Id ,                             -- ID аккаунта
            count(distinct guid) as channels_count  -- Количество каналов
    from `dwh-wazzup`.`dbt_nbespalov`.`int_channels__not_temporary`
    where  deleted=False 
    group by 1
)   -- Таблица c количеством каналов по аккаунту
select *
from count_channels