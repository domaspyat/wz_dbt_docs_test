select  -- Таблица c количеством каналов в разных состояниях в оплаченной подписке
channels.account_id,    -- ID аккаунта
 count(distinct case when billing_packages.paid_at is not null 
 and channels.state='active'
and billing_packages.state='active'
and billing_packages.is_free is distinct from True 
 then channels.guid end) as channels_count_active_in_active_paid_subscription,      -- Количество активных каналов в оплаченной подписке
count(distinct case when billing_packages.paid_at is not null 
and billing_packages.state='active'
and billing_packages.is_free is distinct from True 
 then channels.guid end) as channels_count_any_state_in_active_paid_subscription,   -- Количество каналов в любом состоянии в оплаченной подписке


count(distinct case when channels.transport = 'avito' 
and billing_packages.paid_at is not null 
and billing_packages.state='active'
and billing_packages.is_free is distinct from True 
 then channels.guid end) as channels_avito_count_in_active_paid_subscription, -- Количество каналов конкретного транспорта в оплаченной подписке

 count(distinct case when channels.transport = 'avito' 
 and channels.state='active'
 and billing_packages.state='active'
and billing_packages.paid_at is not null 
and billing_packages.is_free is distinct from True 

 then channels.guid end) as channels_avito_active_count_in_active_paid_subscription -- Количество активных каналов конкретного транспорта в оплаченной подписке
  ,

count(distinct case when channels.transport = 'waba' 
and billing_packages.paid_at is not null 
and billing_packages.state='active'
and billing_packages.is_free is distinct from True 
 then channels.guid end) as channels_waba_count_in_active_paid_subscription, -- Количество каналов конкретного транспорта в оплаченной подписке

 count(distinct case when channels.transport = 'waba' 
 and channels.state='active'
 and billing_packages.state='active'
and billing_packages.paid_at is not null 
and billing_packages.is_free is distinct from True 

 then channels.guid end) as channels_waba_active_count_in_active_paid_subscription -- Количество активных каналов конкретного транспорта в оплаченной подписке
  ,

count(distinct case when channels.transport = 'whatsapp' 
and billing_packages.paid_at is not null 
and billing_packages.state='active'
and billing_packages.is_free is distinct from True 
 then channels.guid end) as channels_whatsapp_count_in_active_paid_subscription, -- Количество каналов конкретного транспорта в оплаченной подписке

 count(distinct case when channels.transport = 'whatsapp' 
 and channels.state='active'
 and billing_packages.state='active'
and billing_packages.paid_at is not null 
and billing_packages.is_free is distinct from True 

 then channels.guid end) as channels_whatsapp_active_count_in_active_paid_subscription -- Количество активных каналов конкретного транспорта в оплаченной подписке
  ,

count(distinct case when channels.transport = 'instagram' 
and billing_packages.paid_at is not null 
and billing_packages.state='active'
and billing_packages.is_free is distinct from True 
 then channels.guid end) as channels_instagram_count_in_active_paid_subscription, -- Количество каналов конкретного транспорта в оплаченной подписке

 count(distinct case when channels.transport = 'instagram' 
 and channels.state='active'
 and billing_packages.state='active'
and billing_packages.paid_at is not null 
and billing_packages.is_free is distinct from True 

 then channels.guid end) as channels_instagram_active_count_in_active_paid_subscription -- Количество активных каналов конкретного транспорта в оплаченной подписке
  ,

count(distinct case when channels.transport = 'tgapi' 
and billing_packages.paid_at is not null 
and billing_packages.state='active'
and billing_packages.is_free is distinct from True 
 then channels.guid end) as channels_tgapi_count_in_active_paid_subscription, -- Количество каналов конкретного транспорта в оплаченной подписке

 count(distinct case when channels.transport = 'tgapi' 
 and channels.state='active'
 and billing_packages.state='active'
and billing_packages.paid_at is not null 
and billing_packages.is_free is distinct from True 

 then channels.guid end) as channels_tgapi_active_count_in_active_paid_subscription -- Количество активных каналов конкретного транспорта в оплаченной подписке
  ,

count(distinct case when channels.transport = 'vk' 
and billing_packages.paid_at is not null 
and billing_packages.state='active'
and billing_packages.is_free is distinct from True 
 then channels.guid end) as channels_vk_count_in_active_paid_subscription, -- Количество каналов конкретного транспорта в оплаченной подписке

 count(distinct case when channels.transport = 'vk' 
 and channels.state='active'
 and billing_packages.state='active'
and billing_packages.paid_at is not null 
and billing_packages.is_free is distinct from True 

 then channels.guid end) as channels_vk_active_count_in_active_paid_subscription -- Количество активных каналов конкретного транспорта в оплаченной подписке
  ,

count(distinct case when channels.transport = 'telegram' 
and billing_packages.paid_at is not null 
and billing_packages.state='active'
and billing_packages.is_free is distinct from True 
 then channels.guid end) as channels_telegram_count_in_active_paid_subscription, -- Количество каналов конкретного транспорта в оплаченной подписке

 count(distinct case when channels.transport = 'telegram' 
 and channels.state='active'
 and billing_packages.state='active'
and billing_packages.paid_at is not null 
and billing_packages.is_free is distinct from True 

 then channels.guid end) as channels_telegram_active_count_in_active_paid_subscription -- Количество активных каналов конкретного транспорта в оплаченной подписке
  




from `dwh-wazzup`.`dbt_nbespalov`.`int_channels__not_temporary` channels
left join  `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` billing_packages
on channels.package_id=billing_packages.guid
where deleted=false
group by 1