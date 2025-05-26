
with old_and_new_data_with_segments as (
    select account_id,  -- ID аккаунта
    who_paid,           -- Кто платил за подписку? возможные значения self, partner, null
    start_date,         -- Дата начала подписки
    start_at,           -- Дата и время начала подписки
    end_date,           -- Дата окончания подписки
    subscription_id,    -- ID подписки
    action,             -- Действие, которое происходит с подпиской: renewal, pay, setPromisedPayment, subtractQuantity, raiseTariff, addQuantity, null
    (case when end_date>=current_date then current_date
    else end_date       
    end) as end_date_corrected from  `dwh-wazzup`.`dbt_nbespalov`.`int_subscrptions_who_paid_old_and_new_data_union_with_who_paid` -- Исправленная дата окончания подписки
    where end_date!=start_date
),

partner_and_type_change as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_type_and_partner_change_with_partner_type_deduplicated`
),
accounts as (select account_Id,             -- ID аккаунта
                        type                -- Тип аккаунта
            from `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts`
),

first_partner_type_with_rn as ( 

select *, row_number() over (partition by account_id order by start_date asc) rn    -- Партиация по ID акканута в порядке возрастания даты начала действия изменения
from partner_and_type_change),

first_partner_type as (
    select * from first_partner_type_with_rn
    where rn =1                         -- Вывод только первых изменений
),

last_partner_date_with_rn as ( 

select *, row_number() over (partition by account_id order by start_date desc) rn 
from partner_and_type_change),

last_partner_dates as (
    select account_id, start_date as last_partner_change_date from last_partner_date_with_rn
    where rn =1
),

first_payment_with_rn as (
    select *, row_number() over (partition by account_id order by start_at asc) rn from old_and_new_data_with_segments
),

first_payment as (
    select * from first_payment_with_rn
    where rn=1
),

last_payment_with_rn as (
    select *, row_number() over (partition by account_id order by start_date desc) rn from old_and_new_data_with_segments
),

last_payments as (
    select account_id, start_date  as last_payment_date from last_payment_with_rn
    where rn=1
),

first_partner_type_up_to_payments as (

select 
first_partner_type.start_date,                                  -- Дата начала действия изменения
cast(first_partner_type.start_date as datetime) as start_at,    -- Дата и время начала действия изменения
cast(null as bool) as has_active_subscription,                  -- Пустое поле для объединения таблиц
cast(null as datetime) as partner_and_type_change_start_at,     -- Пустое поле для объединения таблиц
cast(null as datetime) as end_at,                               -- Пустое поле для объединения таблиц
first_payment.start_date as end_date,                           -- Дата окончания подписки
cast(null as date) as paying_start_date,                        -- Пустое поле для объединения таблиц
cast(null as date) as paying_end_date,                          -- Пустое поле для объединения таблиц
first_partner_type.account_id,                                  -- ID аккаунта
cast(null as string) as action,                                 -- Пустое поле для объединения таблиц
cast(null as string) as who_paid,                               -- Пустое поле для объединения таблиц
cast(null as string) as subscription_id,                        -- Пустое поле для объединения таблиц
first_partner_type.partner_type,                                -- Тип аккаунта партнера
first_partner_type.account_type,                                -- Тип аккаунта
first_partner_type.refparent_id,                                -- ID аккаунта реф. партнера
first_partner_type.partner_id                                   -- ID аккаунта партнера
 from first_partner_type 
left join 
first_payment on first_partner_type.account_id=first_payment.account_id
 where first_partner_type.start_date<first_payment.start_date),


segment_account_type_and_partner_type as (
    select (case when old_and_new_data_with_segments.start_date>=partner_and_type_change.start_date 
    then old_and_new_data_with_segments.start_date 
    else partner_and_type_change.start_date
    end
    ) as start_date,

    (case when old_and_new_data_with_segments.start_at>=partner_and_type_change.start_occured_at
    then old_and_new_data_with_segments.start_at
    else partner_and_type_change.start_occured_at
    end
    ) as start_at, 
    (case when old_and_new_data_with_segments.start_date is not null or  old_and_new_data_with_segments.end_date is not null then True else False end) as has_active_subscription,
    partner_and_type_change.start_occured_at as partner_and_type_change_start_at,
    cast(old_and_new_data_with_segments.end_date as datetime) as end_at,
    coalesce(old_and_new_data_with_segments.end_date, partner_and_type_change.end_date 
    ) as end_date,
    (case when  old_and_new_data_with_segments.start_date is not null  then old_and_new_data_with_segments.start_date
    end) as paying_start_date,

    (case when  old_and_new_data_with_segments.end_date>=current_date then current_date
    else old_and_new_data_with_segments.end_date
    end) as paying_end_date,



    partner_and_type_change.account_id,
    action,
    who_paid,
    subscription_id,
    partner_and_type_change.partner_type,
    partner_and_type_change.account_type,
    partner_and_type_change.refparent_id,
    partner_and_type_change.partner_id
    from partner_and_type_change 
    left join old_and_new_data_with_segments
    on old_and_new_data_with_segments.account_id=partner_and_type_change.account_id
    and old_and_new_data_with_segments.start_date>=partner_and_type_change.start_date
    and old_and_new_data_with_segments.start_date<=partner_and_type_change.end_date),

first_partner_type_and_segment_union as (
    select *, 'data_before_payment' as  data_type  from first_partner_type_up_to_payments
    UNION ALL 
    select *, 'payment_data' as data_type from segment_account_type_and_partner_type
),

first_partner_type_and_segment_union_to_deduplicate as (
    select *, 
    rank() over (partition by account_id, start_at order by partner_and_type_change_start_at desc) rn 
    from first_partner_type_and_segment_union
),

first_partner_type_and_segment_union_deduplicated as (
    select * from first_partner_type_and_segment_union_to_deduplicate
    where rn=1
),

first_partner_type_and_segment_union_deduplicate_subscriptions as (
    select *,  row_number() over (partition by account_id, start_date order by end_date desc,start_at desc) as rn_1 from first_partner_type_and_segment_union_deduplicated
),


first_partner_type_and_segment_union_deduplicated_subscriptions as (
    select * from first_partner_type_and_segment_union_deduplicate_subscriptions
    where rn_1=1
),

first_partner_type_and_segment_union_deduplicated_subscriptions_to_fllnas as (
    select *,
    sum(case when who_paid is not null then 1 end) over (partition by account_id order by start_date) as r_close
    from first_partner_type_and_segment_union_deduplicated_subscriptions

),

first_partner_type_and_segment_union_deduplicated_subscriptions_to_fllnas_with_fill_null as 

(select *, 
        first_value(who_paid) over (partition by account_id,r_close order by start_date asc) as who_paid_filled
        from  first_partner_type_and_segment_union_deduplicated_subscriptions_to_fllnas),



segment_defined as 

(select segment_account_type_and_partner_type.*, (case 
when account_type='partner' then 'partner'
when account_type='child-postpay' then 'child-postpay'
when account_type='tech-partner' then 'tech-partner'
when account_type='tech-partner-postpay' then 'tech-partner-postpay'
when account_type='employee' then 'employee'
when account_type='partner-demo' then 'partner-demo'
when account_type = 'employee' then 'employee'
when who_paid_filled = 'self' and partner_type = 'standart' and account_type = 'standart'  then 'standart_without_partner'
when who_paid_filled='self' and partner_type='partner' then 'of_partner_child_child_paid'
when (who_paid_filled is null or who_paid_filled = 'self') and partner_type is null and account_type='standart' then 'standart_without_partner'
when who_paid_filled='self' and partner_type is null then 'standart_without_partner'
--when who_paid='self' and partner_type is distinct from 'tech-partner' and type='standart' then 'other_types_of_final_clients'
when who_paid_filled='partner' and partner_type is null and type='standart' then 'of_partner_child__of_partner_paid'
when who_paid_filled is null and partner_type='partner' then 'of_partner_child__of_partner_paid'
when who_paid_filled is null and partner_type='tech-partner' then 'tech_partner_child__tech_partner_paid'

when who_paid_filled is null and partner_type='standart' then 'standart_without_partner'

when who_paid_filled='partner' and partner_type='partner' then 'of_partner_child__of_partner_paid'
when who_paid_filled='self' and partner_type='tech-partner' then 'tech_partner_child__child_paid'
when who_paid_filled='partner' and partner_type='tech-partner' then 'tech_partner_child__tech_partner_paid'
when who_paid_filled is null and account_type='standart' and partner_type='tech-partner-postpay' then 'tech_partner_child'
when type='partner' then 'partner'
when type='child-postpay' then 'child-postpay'
when type='tech-partner' then 'tech-partner'
when type='tech-partner-postpay' then 'tech-partner-postpay'
when type='employee' then 'employee'
when type='partner-demo' then 'partner-demo'
when type = 'employee' then 'employee'
else 'unknown'
end) as segment 
from first_partner_type_and_segment_union_deduplicated_subscriptions_to_fllnas_with_fill_null segment_account_type_and_partner_type
left join accounts on segment_account_type_and_partner_type.account_Id = accounts.account_Id
where account_type is not null),

segment_type_with_previous_end_date as (

select *, 
lag(paying_end_date) over (partition by account_id order by start_date) as paying_end_date_previous 
from segment_defined
),

segment_defined_with_has_active_subscription_fixed as (

select *,  
(case when paying_end_date_previous>=end_date then True else has_active_subscription end) 
as has_active_subscription_fixed
from segment_type_with_previous_end_date
)
        -- таблица, которая показывает детальную информацию по изменениям в аккаунтес датой регистрации, датой окончания подписки и информацией по партнерам
select * from segment_defined_with_has_active_subscription_fixed