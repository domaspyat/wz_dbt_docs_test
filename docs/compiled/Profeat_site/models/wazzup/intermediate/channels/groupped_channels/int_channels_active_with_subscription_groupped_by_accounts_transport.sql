  -- Таблица c активными оплаченными каналами после группировки по аккаунту и транспорту
select distinct profile_info.account_id,                                    -- ID аккаунта
                    1 as transport_order_number,                            -- Порядковый номер канала
                  IFNULL(transport,'Нет активного Авито канала') transport  -- Транспорт канала
  from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info
  
  left join `dwh-wazzup`.`dbt_nbespalov`.`int_channels_active_with_active_subscription` channels on profile_info.account_Id = channels.account_Id and transport = 'avito' and paid_At is not null
  

  union all
  
  select distinct profile_info.account_id,
                    3,
                  IFNULL(transport,'Нет активного Instagram канала') transport
  from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info
  left join `dwh-wazzup`.`dbt_nbespalov`.`int_channels_active_with_active_subscription` channels on profile_info.account_Id = channels.account_Id and transport = 'instagram' and paid_At is not null 

union all

 select distinct profile_info.account_id,
                    4,
                  IFNULL(transport,'Нет активного Telegram Bot канала') transport
  from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info
  left join `dwh-wazzup`.`dbt_nbespalov`.`int_channels_active_with_active_subscription` channels on profile_info.account_Id = channels.account_Id 
                                                                                        and transport = 'telegram'
                                                                                        and (
                                                                                            (tariff = 'free' and paid_At is null) 
                                                                                            or
                                                                                            (tariff != 'free' and paid_At is not null)
                                                                                            )

union all

 select distinct profile_info.account_id,
                    5,
                  IFNULL(transport,'Нет активного Telegram Personal канала') transport
  from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info
  left join `dwh-wazzup`.`dbt_nbespalov`.`int_channels_active_with_active_subscription` channels on profile_info.account_Id = channels.account_Id and transport = 'tgapi'  
  union all 

 select distinct profile_info.account_id,
                    6,
                  IFNULL(transport,'Нет активного Вконтакте канала') transport
  from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info
  left join `dwh-wazzup`.`dbt_nbespalov`.`int_channels_active_with_active_subscription` channels on profile_info.account_Id = channels.account_Id 
                                                                                                    and transport = 'vk'
                                                                                                    and (
                                                                                                       (tariff = 'free' and paid_At is null) 
                                                                                                       or
                                                                                                       (tariff != 'free' and paid_At is not null)
                                                                                                       )

  union all


 select distinct profile_info.account_id,
                    7,
                  IFNULL(transport,'Нет активного Waba канала') transport
  from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info
  left join `dwh-wazzup`.`dbt_nbespalov`.`int_channels_active_with_active_subscription` channels on profile_info.account_Id = channels.account_Id and transport in ('waba','wapi') and paid_At is not null 

union all

 select distinct profile_info.account_id,
                    8,
                  IFNULL(transport,'Нет активного Whatsapp Web канала') transport
  from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info
  left join `dwh-wazzup`.`dbt_nbespalov`.`int_channels_active_with_active_subscription` channels on profile_info.account_Id = channels.account_Id and transport = 'whatsapp' and paid_At is not null 
union all

 select distinct profile_info.account_id,
                    9,
                  IFNULL(transport,'Нет активного Viber канала') transport
  from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info
  left join `dwh-wazzup`.`dbt_nbespalov`.`int_channels_active_with_active_subscription` channels on profile_info.account_Id = channels.account_Id and transport = 'viber' and paid_At is not null 


order by account_Id,transport_order_number