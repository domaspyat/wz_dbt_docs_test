

with revenue_amount as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_abcx_segmentation_revenue_amount_with_real_money_spend_on_subscription`
   
),
stg_channels as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_channels`
),
stg_billing_packages as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages`
)
,
account_active_periods_with_revenue_amount_distinct as (
select distinct account_id,
                first_subscription_start,
                last_end_month
from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_abcx_segmentation_users_living_time_with_revenue_periods`

),
profile_info as (
    select *except(account_segment_type)
    from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
),
revenue_amount_values as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_abcx_segmentation_revenue_amount_with_real_money_spend_on_subscription`
 
),
generated_months as (
        select * 
        from `dwh-wazzup`.`dbt_nbespalov`.`stg_months`
        )

,avg_money as (
    select month,
          revenue_amount_values.account_id,
          first_subscription_start,
          lead(month) over (partition by revenue_amount_values.account_id order by month) as next_payment_month,
          revenue_amount
    from revenue_amount_values
    join account_active_periods_with_revenue_amount_distinct accs on revenue_amount_values.account_id = accs.account_id
)
,account_active_periods_with_revenue_amount as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_abcx_segmentation_users_living_time_with_revenue_periods`

),
segments as (
    select   
        account_id,
        date_trunc(date,month) segment_month,
         case when segment ='standart_without_partner' then 'обычный (юзер без партнера)'
                 when segment = 'tech_partner_child__tech_partner_paid' then 'Дочка тех партнёра, которая платит через тех партнёра'
                 when segment = 'tech_partner_child__child_paid' then 'Дочка тех партнёра, которая платит сама'
                 when segment = 'partner' then 'оф. партнёр'
                 when segment = 'employee' then 'работник'
                 when segment = 'partner-demo' then 'демо-партнёр'
                 when segment = 'of_partner_child__of_partner_paid' then 'Дочка, которая платит через партнёра'
                 when segment = 'of_partner_child_child_paid' then 'Самодостаточная дочка оф. партнера , которая платит сама'
                 when segment = 'tech-partner' then 'обычный техпартнер'
                 when segment = 'tech-partner-postpay' then 'техпартнер-постоплатник'
            else 'unknown' end as account_segment_type,
        row_number() over (partition by account_id,date_trunc(date,month) order by date desc) rn_segments
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_who_paid_in_dynamics_defining_clients_with_segments` segments
    join `dwh-wazzup`.`dbt_nbespalov`.`stg_days` days on segments.segment_start <= days.date and days.date < segments.segment_end

)
,calculating_correct_averages as (
    select avg_money.* except(next_payment_month),
            coalesce(next_payment_month,date_trunc(date_add(current_date(),interval 1 month),month)) next_payment_month,
            date_trunc(current_date(),month) as current_month
    --avg(revenue_amount) over (partition by account_id order by avg_money.month rows BETWEEN 5 PRECEDING AND 0 FOLLOWING) avg_sum_in_rubles
            --revenue_amount as avg_sum_in_rubles
    from avg_money
),revenue_average as (
/*генерируем для каждого аккаунта месяца, в которых мы его рассматриваем при подсчете перцинтеля
Мы начинаем учитывать пользователя с его 4 месяца. Например, первая оплата пользователя 2021-06-17, следовательно в месяц 2021-09-01 мы начнем его учитывать.
Поэтому для каждого пользователя мы генерируем его интервалы, то есть месяца, когда он будет считаться при расчете перцентилей.
В момент расчета перцентилей берется «накопившаяся средняя */
    select
          avg_money.account_id,
          avg_money.month as payment_month,
          avg_money.revenue_amount as revenue_amount_in_payment_month,
          next_payment_month,
          months.month as month_since_we_start_considering_user_in_percentile_calculation,
          live_month,
          last_end_month,
          client_living_type,
          active_periods.avg_sum_in_rubles  as avg_sum_in_rubles,
          active_periods.market_type
    from calculating_correct_averages avg_money
    inner join generated_months months on date_trunc(first_subscription_start,month) <= (date_add(months.month,interval -3 month))
                            and months.month >= avg_money.month and (months.month < avg_money.next_payment_month)
    inner join account_active_periods_with_revenue_amount active_periods on avg_money.account_id = active_periods.account_id
    where --months.month <= date_trunc(current_date,month) and
          months.month = live_month --смотрим только те месяца, когда пользователь был активен
),distincts_amount_for_each_account_month as (
    select distinct account_id,
                    market_type,
                    last_end_month,
                    month_since_we_start_considering_user_in_percentile_calculation,
                    avg_sum_in_rubles
    from revenue_average
)
,percentiles as (
select
     segments.account_id,
     last_end_month,
      month_since_we_start_considering_user_in_percentile_calculation,
      PERCENTILE_CONT(avg_sum_in_rubles,0.25) over (partition by month_since_we_start_considering_user_in_percentile_calculation, case when account_segment_type in ('обычный (юзер без партнера)','Самодостаточная дочка оф. партнера , которая платит сама') then 'end_users'
      else account_segment_type end,market_type) AS percentile_25,
      PERCENTILE_CONT(avg_sum_in_rubles,0.75) over (partition by month_since_we_start_considering_user_in_percentile_calculation,case when account_segment_type in ('обычный (юзер без партнера)','Самодостаточная дочка оф. партнера , которая платит сама') then 'end_users'
      else account_segment_type end,market_type) AS percentile_75,
      PERCENTILE_CONT(avg_sum_in_rubles,0.99) over (partition by month_since_we_start_considering_user_in_percentile_calculation,case when account_segment_type in ('обычный (юзер без партнера)','Самодостаточная дочка оф. партнера , которая платит сама') then 'end_users'
      else account_segment_type end,market_type) AS percentile_99
from distincts_amount_for_each_account_month
join segments on distincts_amount_for_each_account_month.account_id = segments.account_id and rn_segments = 1
                                 and distincts_amount_for_each_account_month.month_since_we_start_considering_user_in_percentile_calculation = segments.segment_month
--where account_segment_type in ('обычный (юзер без партнера)','оф. партнёр','обычный техпартнер','Самодостаточная дочка оф. партнера , которая платит сама')

) ,percentile_group as (
--определяем группы по перцентилям

select distinct
                account_active_periods_with_revenue_amount.account_id,
                account_active_periods_with_revenue_amount.live_month,
                percentile_25,
                percentile_75,
                percentile_99,
                account_active_periods_with_revenue_amount.last_end_month,
                avg_sum_in_rubles,
                account_active_periods_with_revenue_amount.market_type,
                case when account_active_periods_with_revenue_amount.client_living_type = 'new' then 'New'
                     when avg_sum_in_rubles<percentile_25 then 'C'
                     when avg_sum_in_rubles<percentile_75 then 'B'
                     when avg_sum_in_rubles<percentile_99 then 'A'
                     when avg_sum_in_rubles>=percentile_99 then 'X'
                end as abcx_segment
from account_active_periods_with_revenue_amount
left join percentiles on account_active_periods_with_revenue_amount.live_month = percentiles.month_since_we_start_considering_user_in_percentile_calculation
                      and account_active_periods_with_revenue_amount.account_id = percentiles.account_id
),final as (
select percentile_group.account_id,         -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts
       
       percentile_group.percentile_25,      -- 25-ый перцентиль средней выручки от пользователя в данном live_month месяце
        percentile_group.percentile_75,     -- 75-ый перцентиль средней выручки от пользователя в данном live_month месяце
       percentile_group.percentile_99,      -- 99-ый перцентиль средней выручки от пользователя в данном live_month месяце
       percentile_group.avg_sum_in_rubles,  -- Скользящая средняя выручка от пользователя за 3 месяца (2 предыдущих + текущий)
       percentile_group.abcx_segment,       -- ABCX сегмент пользователя в данном live_month месяце
       percentile_group.live_month,         -- Месяц жизни пользователя. Формируется на основе истории подписок, формат 2022-11-29
       market_type,                         -- Рынок
       percentile_group.last_end_month,     -- Месяц окончания последней подписки
       count(distinct case when stg_channels.state = 'active' and temporary = False and deleted = False then stg_channels.guid end) as active_channels_count,   -- Количество активных невременных каналов
       count(distinct case when stg_channels.state = 'active' and temporary = False and deleted = False and stg_billing_packages.state = 'active' and paid_At is not null then stg_channels.guid end) as active_channels_count_with_paid_sub,   -- Количество активных невременных каналов с активной платной подпиской
       count(distinct case when temporary = False and deleted = False then stg_channels.guid end ) as channels_count,   -- Количество невременных, неудаленных каналов
       count(distinct case when temporary = False and deleted = False and stg_billing_packages.guid is not null and paid_At is not null then stg_channels.guid end ) as channels_count_with_paid_sub    -- Количество невременных, неудаленных каналов в оплаченной подписке
from percentile_group
join profile_info on percentile_group.account_id = profile_info.account_id
left join stg_channels  on percentile_group.account_id = stg_channels.account_id
left join stg_billing_packages  on stg_channels.package_Id = stg_billing_packages.guid
--where percentile_group.live_month <= date_trunc(current_date(),month)
group by percentile_group.account_id,
       
       percentile_group.last_end_month,    
       percentile_group.percentile_25,      
        percentile_group.percentile_75,     
       percentile_group.percentile_99,      
       percentile_group.avg_sum_in_rubles,  
       percentile_group.abcx_segment,       
       percentile_group.live_month,         
        market_type                        
)   -- Таблица определения ABCX сегмента пользователей без партнёра в динамике. https://www.notion.so/ABCX-7b8f5f7d3e0b470e83fe632828d64821. Все сегменты
select final.*except(abcx_segment),
        case when count(*) over (partition by final.account_id) >1 and live_month = last_end_month then null else abcx_segment end as abcx_segment, -- ABCX сегмент пользователя в данном live_month месяце
               account_segment_type,    -- Сегмент клиента
      case when account_segment_type in ('обычный (юзер без партнера)','Самодостаточная дочка оф. партнера , которая платит сама') then 'Конечный клиент'
           when account_segment_type in ('Дочка тех партнёра, которая платит сама','обычный техпартнер','Дочка тех партнёра, которая платит через тех партнёра') then 'Обычный техпартнер'
           when account_segment_type in ('оф. партнёр','Дочка, которая платит через партнёра') then 'Оф. партнер'
      else account_segment_type end as segment_type_groupped    -- Сегмент группы
from final
join segments on final.account_id = segments.account_id and rn_segments = 1
                                            and final.live_month = segments.segment_month
/*
select *
from dbt_prod.int_subscriptions_lost_revenue_due_to_quantity_and_tariff_change 
where account_id = 32916152


with accounts as (
select distinct lt.account_id,
first_subscription_start,
                c.guid
from dbt_prod.int_accounts_who_paid__standart_russian_users_without_partners_living_time lt
join wazzup.channels c on lt.account_id = c.accountid
where state = 'active' 
      and temporary = False
      and deleted = False
      and packageid is not null
)
select account_id,first_subscription_start,count(distinct guid)
from accounts
where first_subscription_start < '2023-09-01'
group by 1,2
order by 3 desc
limit 100
*/