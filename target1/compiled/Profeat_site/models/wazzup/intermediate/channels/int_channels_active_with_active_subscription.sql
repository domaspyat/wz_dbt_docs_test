-- Таблица c активными каналами и активной подпиской
 select channels.account_Id,    -- ID аккаунта
        channels.guid as channels_in_package,   -- Идентификатор канала
        transport,              -- Транспорт канала
        billingPackages.tariff, -- Тариф
        paid_At                 -- Дата и время оплаты
    from `dwh-wazzup`.`dbt_nbespalov`.`int_channels__not_temporary` channels
    inner join `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` billingPackages 
                                    on billingPackages.guid=channels.package_Id 
    where billingPackages.state='active' 
    --and paid_At is not null 
    and channels.deleted=False
    and channels.state = 'active'