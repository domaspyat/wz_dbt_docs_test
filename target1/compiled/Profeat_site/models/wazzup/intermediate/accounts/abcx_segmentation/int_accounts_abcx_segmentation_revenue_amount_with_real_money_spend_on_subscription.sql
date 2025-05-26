

with int_subscription_updates__corect_date_and_filledna_tarif_period_quantity as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscription_updates__corect_date_and_filledna_tarif_period_quantity`
),
raise_info as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_only_subscription_payments_and_wapi_sessions_real_money_with_subscription_updates`
),
stg_billing_packages as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages`
),
int_accounts_profile_info as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
),
stg_months as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_months`
),
stg_accounts as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts`
),
affiliates as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_affiliates`)

,all_regular_payments as (
    select date_trunc(cast(subscription_info.paid_at as date),month) as paid_month,
           cast(subscription_info.paid_at as date) paid_date,
            subscription_info.paid_at,
                    /*case when coalesce(new_until_expired_days,until_expired_days) is not null
                    then date_add(cast(subscription_info.paid_at as date),interval coalesce(new_until_expired_days,until_expired_days) day)
                    else date_add(cast(subscription_info.paid_at as date),interval 1 month)
                    end as till_what_date,*/

            case when coalesce(period_new,period,1) = 1 and
                    date_diff(
                                case when coalesce(new_until_expired_days,until_expired_days) is not null
                                    then date_add(cast(subscription_info.paid_at as date),interval coalesce(new_until_expired_days,until_expired_days) day)
                                    else date_add(cast(subscription_info.paid_at as date),interval 1 month)
                                end,
                                cast(subscription_info.paid_at as date),month)  > 1
                    then date_add(cast(subscription_info.paid_at as date),interval 1 month)
                    else case when coalesce(new_until_expired_days,until_expired_days) is not null
                                then date_add(cast(subscription_info.paid_at as date),interval coalesce(new_until_expired_days,until_expired_days) day)
                         else date_add(cast(subscription_info.paid_at as date),interval 1 month)
                         end
                         end as till_what_date,
              
                        case when coalesce(new_until_expired_days,until_expired_days) is not null
                                then date_add(cast(subscription_info.paid_at as date),interval coalesce(new_until_expired_days,until_expired_days) day)
                         else date_add(cast(subscription_info.paid_at as date),interval 1 month)
                         end as till_what_date_without_one_month_condition,



                    date_add(date_trunc(case when coalesce(period_new,period,1) = 1 and
                    date_diff(
                    case when coalesce(new_until_expired_days,until_expired_days) is not null
                            then date_add(cast(subscription_info.paid_at as date),interval coalesce(new_until_expired_days,until_expired_days) day)
                         else date_add(cast(subscription_info.paid_at as date),interval 1 month)
                         end,cast(subscription_info.paid_at as date),month) >1
                        then date_add(cast(subscription_info.paid_at as date),interval 1 month)
                        else case when coalesce(new_until_expired_days,until_expired_days) is not null
                        then date_add(cast(subscription_info.paid_at as date),interval coalesce(new_until_expired_days,until_expired_days) day)
                        else date_add(cast(subscription_info.paid_at as date),interval 1 month)
                        end 
                                end,month),
                                                 interval - 1 day) as till_what_date_fixed_end,

                    int_accounts_profile_info.account_Id,
                    raise_info.action,
                    raise_info.subscription_id,
                    raise_info.sum_in_rubles_spent_on_subscription,
                    case when stg_accounts.currency in ('RUR','KZT') then 'ru' else 'global' end as market_type,
                    coalesce(new_until_expired_days,until_expired_days,30) until_expired,
                    coalesce(period_new,period,1) period,
                    --raise_info.sum_in_rubles_spent_on_subscription/coalesce(new_until_expired_days,until_expired_days,30) sum_per_day,
                    account_segment_type as account_type,
                    int_accounts_profile_info.register_date
       from raise_info
       join int_subscription_updates__corect_date_and_filledna_tarif_period_quantity subscription_info on raise_info.subscription_update_id = subscription_info.guid
       join stg_billing_packages on subscription_info.subscription_id = stg_billing_packages.guid
       join int_accounts_profile_info on stg_billing_packages.account_id = int_accounts_profile_info .account_id
       join stg_accounts on raise_info.account_id = stg_accounts.account_Id
       left join affiliates affs on raise_info.account_id = affs.child_id
       where raise_info.action not in ('raiseTariff','addQuantity')
                --and raise_info.account_id = 71372436
                --order by 2
                )
,combine_data_in_one_month_ as (
  select row_number() over (partition by paid_month,subscription_id order by paid_at  desc) rn,
        coalesce(lag(paid_date) over (partition by paid_month,subscription_id order by paid_at),paid_date) paid_date,
        coalesce(lag(paid_at) over (partition by paid_month,subscription_id order by paid_at),paid_at) paid_at,
        sum(sum_in_rubles_spent_on_subscription) over (partition by paid_month,subscription_id order by paid_at asc) sum_in_rubles_spent_on_subscription,
        sum(period) over (partition by paid_month,subscription_id order by paid_at asc) periods_in_month,
        * except(paid_date,paid_at,sum_in_rubles_spent_on_subscription)
        
  from all_regular_payments
order by subscription_id,paid_at
),combine_data_in_one_month as (
  select *,
  case when paid_date < lag(till_what_date_without_one_month_condition) over (partition by subscription_id order by paid_date) then TRUE else False end as  was_active
  from combine_data_in_one_month_
  where rn = 1
)
/*
select *
from combine_data_in_one_month
--where rn = 1
*/
,defining_correct_paid_dates as (
    /*
    Это для оплат, которые были в период уже активной подписки.
    */
     select 
subscription_id,
            last_day(lag(till_what_date_without_one_month_condition) over (partition by subscription_id order by paid_at),month) ,
case 
    when (was_active is TRUE or periods_in_month != period) then      
                      case when period = 1 and date_diff(till_what_date_without_one_month_condition,paid_date,month) = 2 then paid_date
                           when
                            paid_date < lag(till_what_date_without_one_month_condition) over (partition by subscription_id order by paid_at) and lag(till_what_date) over (partition by subscription_id order by      paid_at) != last_day(lag(till_what_date) over (partition by subscription_id order by paid_at),month) 
                      then
                             date_add(lag(till_what_date_without_one_month_condition) over (partition by subscription_id order by paid_at),interval 1 day) 
                      when paid_date < lag(till_what_date_without_one_month_condition) over (partition by subscription_id order by paid_at) and lag(till_what_date) over (partition by subscription_id order by paid_at) = last_day(lag(till_what_date_without_one_month_condition) over (partition by subscription_id order by paid_at),month) 
                      then
                             lag(till_what_date_without_one_month_condition) over (partition by subscription_id order by paid_at)
                               else paid_date end

        when (periods_in_month = period or was_active is false) then
                    case when 
                            paid_date < lag(till_what_date) over (partition by subscription_id order by paid_at) and lag(till_what_date) over (partition by subscription_id order by paid_at) != last_day(lag(till_what_date) over (partition by subscription_id order by paid_at),month) 
                      then
                             date_add(lag(till_what_date) over (partition by subscription_id order by paid_at),interval 1 day) 
                      when paid_date < lag(till_what_date) over (partition by subscription_id order by paid_at) and lag(till_what_date) over (partition by subscription_id order by paid_at) = last_day(lag(till_what_date) over (partition by subscription_id order by paid_at),month) 
                      then
                             lag(till_what_date) over (partition by subscription_id order by paid_at)
                               else paid_date end 

   end as paid_date_fixed,
                         period,
                         paid_date,
            sum_in_rubles_spent_on_subscription,
            till_what_date,
            till_what_date_without_one_month_condition,
            case when date_diff(till_what_date_without_one_month_condition,paid_date,month) = 0 then till_what_date_without_one_month_condition
                 when  was_active is TRUE or periods_in_month != period  
                          then date_add(date_trunc(till_what_date_without_one_month_condition,month),interval -1 day)
                 when  periods_in_month = period or was_active is false then  till_what_date_fixed_end 
                      else date_add(date_trunc(till_what_date_without_one_month_condition,month),interval -1 day) end as till_what_date_fixed_end,
            periods_in_month,
            account_id,
            until_expired,
            account_type,
            was_active,
                        register_date,
                        market_type
from combine_data_in_one_month
where rn =1
order by subscription_id,paid_at
--and account_id =
),
    prepare_to_correct_paid_dates as (
        select *except(till_what_date_fixed_end),
        case when paid_date_fixed > till_what_date_fixed_end then date_add(date_trunc(till_what_date_without_one_month_condition,month),interval -1 day)
        else till_what_date_fixed_end end as till_what_date_fixed_end
        from defining_correct_paid_dates
    ),
    metrics_to_correct_paid_dates as (
   select *,
        date_diff(paid_date_fixed, lag(till_what_date_fixed_end) over (partition by subscription_id order by paid_date),month) pfixed,
        date_diff(paid_date, lag(paid_date) over (partition by subscription_id order by paid_date),month) pm
    from prepare_to_correct_paid_dates 
    ),   
     correcting_paid_dates  as (
        select *except(paid_date_fixed),
        case when pfixed = 2 and pm = 1 then paid_date else paid_date_fixed end as paid_date_fixed
        from metrics_to_correct_paid_dates 
    ),defining_correct_until_expired_days as (
        select *,

        /*(case when date_diff(till_what_date_without_one_month_condition,paid_date,month) = 0
                then  until_expired
             else date_diff(till_what_date_fixed_end,paid_date_fixed,day)+1 end until_expired_fixed,
        sum_in_rubles_spent_on_subscription,
        sum_in_rubles_spent_on_subscription/(  case when date_diff(till_what_date_without_one_month_condition,paid_date,month) = 0
                                                then  until_expired
                                                else date_diff(till_what_date_fixed_end,paid_date_fixed,day)+1 end) sum_per_day*/
 case when date_diff(till_what_date_without_one_month_condition,paid_date,month) = 0
                then  1
     when date_diff(till_what_date_without_one_month_condition,paid_date_fixed,month) = 0
                then 1
                else date_diff(till_what_date_fixed_end,paid_date_fixed,month)+1 end until_expired_fixed,

        sum_in_rubles_spent_on_subscription,
        sum_in_rubles_spent_on_subscription/(case when date_diff(till_what_date_without_one_month_condition,paid_date,month) = 0
                                                      then  1
                                            when date_diff(till_what_date_without_one_month_condition,paid_date_fixed,month) = 0
                                                then 1
                                             else date_diff(till_what_date_fixed_end,paid_date_fixed,month)+1 end) sum_per_day


        from correcting_paid_dates

)
,joining_subscription_history as (
            select  subs_info.*,
            paid_date_fixed,
                        month,
                        sum_per_day,
                        till_what_date_fixed_end,
                        account_type,
                        register_date,
                        market_type
            from `dwh-wazzup`.`dbt_nbespalov`.`int_subscription_deduplicated_by_id_combined_intervals_only_paid_fixed_end` subs_info
            join stg_months mnts on mnts.month >= date_trunc(subscription_start,month) 
                                                        and mnts.month < date_trunc(subscription_end,month)
            join defining_correct_until_expired_days defining_correct_until_expired_days on defining_correct_until_expired_days.subscription_id = subs_info.subscription_id
                                    and defining_correct_until_expired_days.paid_date_fixed >= subscription_start and defining_correct_until_expired_days.paid_date_fixed <= subscription_end
                                      and date_trunc(paid_date_fixed,month) <= month and month <= date_trunc(till_what_date_fixed_end,month) 
            --order by paid_date_fixed,month

            --join `dwh-wazzup`.`dbt_swazzup`.`stg_days` days on paid_date_fixed <= days.date and days.date <= till_what_date_fixed_end      and date_trunc(days.date,month) = mnts.month
            
)
/*,subscription_history_by_month as (
select 
                  sum(sum_per_day) as sum_earned_by_period  
        --sum_in_rubles_spent_on_subscription/count(*) over (partition by subscription_id,subscription_start,subscription_end ) sum_earned_by_period
from joining_subscription_history
join stg_months mnts on mnts.month >= date_trunc(subscription_start,month) 
                                            and mnts.month < date_trunc(subscription_end,month)
) */
,regular_payments_groupped_data  as (
select account_id,
        account_type,
        register_date,
        market_type,
        month,
        sum(sum_per_day) sum_in_rubles_spent_on_subscription_period
from joining_subscription_history
group by account_id,
        register_date,
         account_type,
          market_type,
            month)
,
raise_add as (
                select raise_info.paid_date,
                        int_accounts_profile_info.account_Id,
                        raise_info.action,
                        raise_info.subscription_id,
                        raise_info.subscription_update_id,
                        raise_info.sum_in_rubles_spent_on_subscription,
                        subscription_info.until_expired_days,
                         case when stg_accounts.currency in ('RUR','KZT') then 'ru' else 'global' end as market_type,
                        account_segment_type as account_type,
                             int_accounts_profile_info.register_date        
                from raise_info
                join int_subscription_updates__corect_date_and_filledna_tarif_period_quantity subscription_info on raise_info.subscription_update_id = subscription_info.guid
                join stg_billing_packages on subscription_info.subscription_id = stg_billing_packages.guid
                join int_accounts_profile_info on stg_billing_packages.account_id = int_accounts_profile_info.account_id
                 join stg_accounts on raise_info.account_id = stg_accounts.account_Id
                left join affiliates affs on raise_info.account_id = affs.child_id
                where raise_info.action  in ('raiseTariff','addQuantity')
                        
                --and date_trunc(ra.paid_date,month) >= '2023-06-01'
), upgrade_months_info as (
                select account_id,
                        account_type,
                        market_type,
                        register_date,
                        date_trunc(paid_date,month) upgrade_month,
                        date_trunc(date_add(paid_date,interval until_expired_days day),month) till_what_month_upgrade,
                        case when date_diff(date_trunc(date_add(paid_date,interval until_expired_days day),month),date_trunc(paid_date,month),month) = 0 then 1
                        else date_diff(date_trunc(date_add(paid_date,interval until_expired_days day),month),date_trunc(paid_date,month),month)
                        end upgrade_months_active_count,
                        sum_in_rubles_spent_on_subscription as upgrade_sum
                from raise_add
),
groupped_data as (
                select  account_id,
                        account_type,
                        market_type,
                        register_date,
                        upgrade_month,
                        upgrade_months_info.till_what_month_upgrade,
                        upgrade_months_info.upgrade_months_active_count,
                        sum(upgrade_sum) upgrade_sum_in_month,
                        sum(upgrade_sum)/upgrade_months_info.upgrade_months_active_count as upgrade_sum_monthly
                from upgrade_months_info
                where upgrade_month is not null
                group by 1,2,3,4,5,6,7),
raise_add_groupped_data as (
                select  account_id,
                        account_type,
                        market_type,
                        register_date,
                        month,
                        sum(upgrade_sum_monthly) upgrade_sum_monthly
                from groupped_data
                join stg_months months on upgrade_month <= months.month
                and months.month <= till_what_month_upgrade
                --and upgrade_sum_monthly != 0 
                group by 1,2,3,4,5)
select  regular_payments_groupped_data.month,           -- Месяц, сгенерированный на основе дате выручки и expired_days, формат 2022-11-29
        regular_payments_groupped_data.account_id,      -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts
        regular_payments_groupped_data.account_type,
        regular_payments_groupped_data.market_type,
        regular_payments_groupped_data.register_date,
        sum(sum_in_rubles_spent_on_subscription_period+coalesce(upgrade_sum_monthly,0)) revenue_amount  -- Размер средней выручки (выручка/период) в рассматриваемом месяце
from regular_payments_groupped_data 
left join raise_add_groupped_data on regular_payments_groupped_data.account_id = raise_add_groupped_data.account_id
                                    and regular_payments_groupped_data.month = raise_add_groupped_data.month
group by 1,2,3,4,5
    -- Таблица с выручкой от пользователя и её динамическим распределением с учетом повышение тарифов. Под динамическим распределением понимается разделение выручки на купленный период. Например, пользователь купил подписку за 10к рублей 2023-05-05 на 5 месяцев.То есть в среднем его выручка в каждый месяц составляет 2к рублей май,июнь,июль,август,сентябрь и, если пользователь не внёс дополнительно деньги, то октябрь, иначе учитывается новая выручка от клиента