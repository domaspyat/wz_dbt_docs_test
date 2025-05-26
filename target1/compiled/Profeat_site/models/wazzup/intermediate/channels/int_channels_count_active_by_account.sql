with count_active as (
    select channels.account_Id,     -- ID аккаунта
            count(distinct channels.guid) as channels_in_package    -- Количество каналов в подписке
    from `dwh-wazzup`.`dbt_nbespalov`.`int_channels__not_temporary` channels
    inner join `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` billingPackages 
                                    on billingPackages.guid=channels.package_Id 
    where billingPackages.state='active' 
    and 
       ((billingPackages.tariff = 'free' and paid_At is null) 
                                               or
       (billingPackages.tariff != 'free' and paid_At is not null))                                          
    and channels.deleted=False
    group by 1)
select *    -- Таблица c количеством активных каналов по аккаунту
from count_active