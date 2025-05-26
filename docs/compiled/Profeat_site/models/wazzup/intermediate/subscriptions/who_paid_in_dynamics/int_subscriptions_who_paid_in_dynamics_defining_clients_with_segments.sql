with segments as (
    select  subscription_start as segment_start,
            subscription_end_with_last_payment_date as segment_end,
            account_id, -- ID аккаунта
            segment 
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_who_paid_in_dynamics_combined_intervals_all_segments`

), 

client_types as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_defining_clients_types`

),

defining_clients_with_segments as (
    select coalesce(client_types.account_id, segments.account_id) as account_id,
    coalesce((case when subscription_start>=segment_start then subscription_start end), segment_start
    ) as subscription_start,                    -- Дата начала подписки
    subscription_start as active_period_start,  -- Дата начала активного периода
    coalesce((case when subscription_end<=segment_end then subscription_end end),
    segment_end)  as subscription_end,          -- Дата окончания подписки
    subscription_end as active_period_end,      -- Дата окончания активного периода
    segment_start,  -- Дата начала сегмента
    segment_end,    -- Дата окончания сегмента
    segment,        -- Сегмент: of_partner_child__of_partner_paid, of_partner_child_child_paid, unknown, standart_without_partner, tech_partner_child__child_paid, tech_partner_child__tech_partner_paid, partner, tech-partner, employee, tech-partner-postpay
    has_paid        -- Клиент нам платил?
     from client_types
    full outer join  segments
    on segments.account_id=client_types.account_id
    and segments.segment_start<=client_types.subscription_end
    where segment is not null and subscription_start is not null),


defining_clients_with_segments_to_deduplicate as (
    select *, row_number() over (partition by account_id, subscription_start order by segment_start desc) as rn 
    from defining_clients_with_segments
),

segments_final as (
    select * from defining_clients_with_segments_to_deduplicate
    where rn=1  -- Показываем, начиная с самых поздних изменений по segment_start
)
    -- Таблица, которая показывает сегмент клиента и даты этого сегмента, а также активность по подпискам
select *, (case when subscription_end>=current_date then current_date
    else subscription_end   -- Либо дата окончания подписки, либо текущая дата (если подписка заканчивается позже чем сегодня)
    end) as subscription_end_fixed from segments_final