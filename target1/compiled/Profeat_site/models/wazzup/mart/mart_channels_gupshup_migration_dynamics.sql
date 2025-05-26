with snapshot as (
    select *
    from `dwh-wazzup`.`snapshots`.`channels_snapshot`
    where temporary = False
    and deleted = False
)   -- Таблица с каналами, которые перешли с Key-Reply на GupShup
select date_trunc(dbt_valid_to,week(monday)) week_dt,   -- Неделя перехода с Key-Reply на GupShup
        count(distinct guid) migrated_channels_count,   -- Количество перешедших каналов
from snapshot   
where dbt_valid_to is not null
group by 1