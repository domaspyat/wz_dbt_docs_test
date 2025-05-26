with childs_registration_activation_info as (
  select partner_id,
        reg_month,
        converted_daughters_count as activated_users,
        all_daughters_count as registrations_count,
  from  `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partners_metrics_for_partners_reg_to_active`
  where type = 'monthly'
),
    payments_info as (
  select distinct partner_id,
          month as first_payment_month
  from `dwh-wazzup`.`dbt_nbespalov`.`mart_key_partner_metrics_aggregated`
  where event in('payment','tech_partner_payment') 

),
    revenue_info as (


--first version
/*
  select partner_id,
          value as revenue_amount,
          month as revenue_month
  from dbt_prod.mart_partners_metrics_by_month_dynamics 
  where metrics = 'sum_in_rubles_partner_paid' --выручка от партнера
            --and metrics = 'sum_in_rubles' --выручка от партенра + клиента
*/
select 
              partner_id,
              month as revenue_month,
              sum(sum) as revenue_amount,
              sum(waba_sum_in_rubles) as waba_revenue_amount
              
  from `dwh-wazzup`.`dbt_nbespalov`.`mart_key_partner_metrics_aggregated`
  where event in ('revenue') 
  group by partner_id,month


), mart_partners_key_metrics_for_hypothesis as (
  select partner_id,
         date as month,
         sum(case when metric = 'daughters_count' then value end) active_users_count, -- количество активных дочек
         sum(case when metric = 'paid_channels_quantity' then value end) sold_channels_count, --количество купленных каналов в этом месяце
  from  `dwh-wazzup`.`dbt_nbespalov`.`mart_partners_key_metrics_for_hypothesis`
  where type = 'monthly'
  group by partner_id,
                date
), /*avg_price_info as ( --не нужен
  select partner_id,
          paid_month as avg_price_month,
          sum_in_rubles,--брать из динамики партнеров выручку от партнера
          distinct_users_count
  from dbt_swazzup.int_accounts_partners_revenue_by_month
  where partner_id is not null
),
*/
amocrm_info as (
  select distinct company_id,
                name,
                responsible_user_id,
                replace(account_number,'-','') account_number,
                created_at,
                updated_at,
                row_number() over (partition by account_number order by created_at desc) rn,
                manager_name
from `dwh-wazzup`.`wazzup`.`amocrm_companies` amocrm_companies
join `dwh-wazzup`.`wazzup`.`sales_managers` sales_managers on amocrm_companies.responsible_user_id = sales_managers.manager_code

)
    -- Выручка менеджеров по продажам из партнерки
select 
        manager_name,                                       -- Имя нашего менеджера
       profile_info.account_id as partner_id,               -- ID аккаунта партнера
       stg_months.month,                                    -- Рассматриваемый месяц
        case when stg_months.month = date_trunc(partner_register_date,month)  then profile_info.account_id end  partnerships_count, -- Количество партнерок
        registrations_count,                                -- Количество регистраций
        payments_info.partner_id as first_payments_count,   -- Количество первых оплат
        revenue_info.partner_id as partner_id_for_revenue,  -- ID партнера для выручки
        revenue_amount revenue_amount,                      -- Сумма выручки
        waba_revenue_amount as waba_revenue_amount,         -- Сумма выручки за WABA
        activated_users,                                    -- Активиривано юзеров
       active_users_count as active_users_count,            -- Количество активных юзеров
       sold_channels_count as sold_channels_count           -- Количество проданных каналов
from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info
join amocrm_info on cast(profile_info.account_id as string) = amocrm_info.account_number and rn = 1
join `dwh-wazzup`.`dbt_nbespalov`.`stg_months` stg_months on   stg_months.month >=  date_trunc(partner_register_date,month)
                            and stg_months.month <= date_trunc(current_date(),month)

left join  payments_info on  payments_info.first_payment_month = stg_months.month 
                              and payments_info.partner_id = profile_info.account_id 

left join childs_registration_activation_info regs_to_active_info 
                                                                    on regs_to_active_info.partner_id = profile_info.account_id
                                                                        and regs_to_active_info.reg_month = stg_months.month
left join revenue_info on 
                            profile_info.account_Id = revenue_info.partner_id
                            and stg_months.month = revenue_info.revenue_month

left join mart_partners_key_metrics_for_hypothesis on profile_info.account_id = mart_partners_key_metrics_for_hypothesis.partner_id
                                                    and stg_months.month = mart_partners_key_metrics_for_hypothesis.month

where profile_info.type = case when manager_name in ('Инна Бородина','Василий Прасолов') then 'tech-partner' else 'partner' end
      and currency in ('RUR','KZT')