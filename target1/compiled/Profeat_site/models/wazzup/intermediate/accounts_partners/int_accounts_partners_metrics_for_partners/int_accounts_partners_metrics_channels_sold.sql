

with last_value_tariff as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_by_subscription_id_tarif_period_quantity`
),
last_value_tarif_by_subscription_id as (
    select subscription_id,
            subscription_type,
            data_source,
            partner_id,
            paid_month, 
            tariff_new,
            period_new,
            quantity_new 

    from last_value_tariff
    where tariff_new is not null
    group by 1,2,3,4,5,6,7,8
),
union_types as (
 select partner_id,
              paid_month,           -- Месяц оплаты
              'monthly' as type,    -- Тип временного периода
  sum(quantity_new) as paid_channels_quantity,
   sum(case when subscription_type in ('waba','wapi') then quantity_new end) as paid_channels_waba_quantity,
   sum(case when subscription_type='tgapi' then quantity_new end) as paid_channels_tgapi_quantity,
   sum(case when subscription_type='whatsapp' then quantity_new end) as paid_channels_wa_quantity,
    sum(case when subscription_type='telegram' then quantity_new end) as paid_channels_telegram_quantity,
    sum(case when subscription_type='instagram' then quantity_new end) as paid_channels_instagram_quantity,
    sum(case when subscription_type='avito' then quantity_new end) as paid_channels_avito_quantity,
    sum(case when subscription_type= 'vk' then quantity_new end) as paid_channels_vk_quantity,
    sum(case when subscription_type= 'viber' then quantity_new end) as paid_channels_viber_quantity

  from last_value_tarif_by_subscription_id
  where data_source = 'partner_payment'
  group by 1,2

union all
select partner_id,              -- ID аккаунта партнера
              current_date(),
              'all' as type,    -- Тип временного периода
  sum(quantity_new) as paid_channels_quantity,  -- Количество оплаченных каналов
   sum(case when subscription_type in ('waba','wapi') then quantity_new end) as paid_channels_waba_quantity,    -- Количество оплаченных каналов WABA
   sum(case when subscription_type='tgapi' then quantity_new end) as paid_channels_tgapi_quantity,              -- Количество оплаченных каналов TGAPI
   sum(case when subscription_type='whatsapp' then quantity_new end) as paid_channels_wa_quantity,              -- Количество оплаченных каналов WHATSAPP
    sum(case when subscription_type='telegram' then quantity_new end) as paid_channels_telegram_quantity,       -- Количество оплаченных каналов TELEGRAM
    sum(case when subscription_type='instagram' then quantity_new end) as paid_channels_instagram_quantity,     -- Количество оплаченных каналов INSTAGRAM
    sum(case when subscription_type='avito' then quantity_new end) as paid_channels_avito_quantity,             -- Количество оплаченных каналов AVITO
    sum(case when subscription_type= 'vk' then quantity_new end) as paid_channels_vk_quantity,                  -- Количество оплаченных каналов VK
    sum(case when subscription_type= 'viber' then quantity_new end) as paid_channels_viber_quantity             -- Количество оплаченных каналов VIBER

  from last_value_tarif_by_subscription_id
  where data_source = 'partner_payment'
  group by 1
)   -- Количество проданных партнерами каналов
select *
from union_types