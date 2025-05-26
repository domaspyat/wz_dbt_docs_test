  -- Таблица с каналами после группировки по аккаунту и транспорту
select distinct profile_info.account_id,                                -- ID аккаунта
                    1 as transport_order_number,                        -- Порядковый номер канала
                  IFNULL(transport,'Не было Авито канала') transport    -- Транспорт канала
  from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info
  left join `dwh-wazzup`.`dbt_nbespalov`.`int_channels__not_temporary` channels on profile_info.account_Id = channels.account_Id and transport = 'avito' and deleted = false
  

  union all
  
  select distinct profile_info.account_id,
                    3,
                  IFNULL(transport,'Не было Instagram канала') transport
  from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info
  left join `dwh-wazzup`.`dbt_nbespalov`.`int_channels__not_temporary` channels on profile_info.account_Id = channels.account_Id and transport = 'instagram' and deleted = false

union all

 select distinct profile_info.account_id,
                    4,
                  IFNULL(transport,'Не было Telegram Bot канала') transport
  from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info
  left join `dwh-wazzup`.`dbt_nbespalov`.`int_channels__not_temporary` channels on profile_info.account_Id = channels.account_Id and transport = 'telegram' and deleted = false

union all

 select distinct profile_info.account_id,
                    5,
                  IFNULL(transport,'Не было Telegram Personal канала') transport
  from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info
  left join `dwh-wazzup`.`dbt_nbespalov`.`int_channels__not_temporary` channels on profile_info.account_Id = channels.account_Id and transport = 'tgapi' and deleted = false

  union all 

 select distinct profile_info.account_id,
                    6,
                  IFNULL(transport,'Не было Вконтакте канала') transport
  from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info
  left join `dwh-wazzup`.`dbt_nbespalov`.`int_channels__not_temporary` channels on profile_info.account_Id = channels.account_Id and transport = 'vk' and deleted = false

  union all

 select distinct profile_info.account_id,
                    7,
                  IFNULL(transport,'Не было Waba канала') transport
  from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info
  left join `dwh-wazzup`.`dbt_nbespalov`.`int_channels__not_temporary` channels on profile_info.account_Id = channels.account_Id and transport in ('waba','wapi') and deleted = false

union all

 select distinct profile_info.account_id,
                    8,
                  IFNULL(transport,'Не было Whatsapp Web канала') transport
  from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info
  left join `dwh-wazzup`.`dbt_nbespalov`.`int_channels__not_temporary` channels on profile_info.account_Id = channels.account_Id and transport = 'whatsapp' and deleted = false

union all

 select distinct profile_info.account_id,
                    9,
                  IFNULL(transport,'Не было Viber канала') transport
  from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info
  left join `dwh-wazzup`.`dbt_nbespalov`.`int_channels__not_temporary` channels on profile_info.account_Id = channels.account_Id and transport = 'viber' and deleted = false