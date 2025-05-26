select account_id,

count(distinct case when type='whatsapp' and state='active' and paid_at is not null then guid  end) as whatsapp_active_paid,
count(distinct case when type='instagram' and state='active' and paid_at is not null then guid  end) as instagram_active_paid,
count(distinct case when type='telegram' and state='active' and paid_at is not null then guid  end) as telegram_bot_active_paid,
count(distinct case when type='tgapi' and state='active' and paid_at is not null then guid end) as telegram_personal_active_paid,
count(distinct case when type='avito'  and state='active' and paid_at is not null then guid end) as avito_active_paid,
count(distinct case when type='vk'  and state='active' and paid_at is not null then guid end) as vk_active_paid,
count(distinct case when type='whatsapp' and paid_at is not null then guid end) as whatsapp_paid,
count(distinct case when type='instagram' and paid_at is not null then guid end) as instagram_paid,
count(distinct case when type='telegram' and paid_at is not null then guid end) as telegram_bot_paid,
count(distinct case when type='tgapi' and paid_at is not null then guid end) as telegram_personal_paid,
count(distinct case when type='avito'  and paid_at is not null then guid end) as avito_paid,
count(distinct case when type='vk' and paid_at is not null then guid end) as vk_paid
from `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages`
group by 1