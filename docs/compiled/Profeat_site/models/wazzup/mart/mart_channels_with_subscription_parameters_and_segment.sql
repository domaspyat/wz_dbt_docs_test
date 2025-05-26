with subscription_with_parameters as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_by_subscription_id_tarif_period_quantity_by_dates`
),

active_channels_with_tarifs as (
  select channels.channel_id,
  cast(channels.date as date) as date,
  transport,
  channels.account_id,
  subscription_id,
  segment,
  is_free,
  (case when segment in ('of_partner_child__of_partner_paid') then 'дочки оф. партнера'
  when segment in ('tech_partner_child__child_paid','tech_partner_child__tech_partner_paid') then 'клиенты тех. партнера'
  when segment in ('standart_without_partner','of_partner_child_child_paid') then 'конечные клиенты'
  end) as segment_aggregated,
  accounts_by_days_by_segment.currency
  from  `dwh-wazzup`.`dbt_nbespalov`.`int_channels_active_channels` channels
  left join `dwh-wazzup`.`dbt_nbespalov`.`mart_active_accounts_by_days_by_segment`  accounts_by_days_by_segment 
  on accounts_by_days_by_segment.date=cast(channels.date as date)
  and accounts_by_days_by_segment.account_id=channels.account_id  
    
),

tarif_info_aggregated_with_channel_info as (
    select subscription_with_parameters.*,
    active_channels_with_tarifs.channel_id,                         -- ID канала
    active_channels_with_tarifs.transport,                          -- Транспорт
    active_channels_with_tarifs.segment,                            -- Сегмент клиента
    active_channels_with_tarifs.segment_aggregated,                 -- Сегмент клиента после группировки
    active_channels_with_tarifs.is_free,                            -- Бесплатный ли канал (подписка)?
    active_channels_with_tarifs.date as channel_date,               -- Рассматриваемая дата активного канала с подпиской
    active_channels_with_tarifs.account_id as channel_account_id    -- Номер аккаунт активного канала с подпиской
    from active_channels_with_tarifs 
    left join subscription_with_parameters
    on active_channels_with_tarifs.account_id=subscription_with_parameters.account_id 
    and active_channels_with_tarifs.date=subscription_with_parameters.date
    and active_channels_with_tarifs.subscription_id=subscription_with_parameters.subscription_id
)
    -- Каналы с параметрами подписки и сегментом
select * from tarif_info_aggregated_with_channel_info
where segment is not null and is_free is distinct from true