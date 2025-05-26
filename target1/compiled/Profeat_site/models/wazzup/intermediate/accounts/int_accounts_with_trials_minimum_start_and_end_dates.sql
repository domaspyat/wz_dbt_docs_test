with accounts_with_trials as (select account_id,
                                whatsap_trial_start,
                                whatsap_trial,
                                instagram_trial,
                                instagram_trial_start,
                                avito_trial,
                                avito_trial_start,
                                vk_trial_start,
                                vk_trial,
                                telegram_trial,
                                telegram_trial_start,
                                tgapi_trial,
                                tgapi_trial_start
                            from `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts`)
 ,channels_waba_trial as (
        select account_id,
                min(cast(datetime(created_at,'Europe/Moscow') as date))  as trial_end 
        from `dwh-wazzup`.`dbt_nbespalov`.`stg_channels`
        where temporary=False 
              and transport in ('waba','wapi')
        group by 1
 )
,trials_end_unpivot as (
        SELECT * 
        FROM accounts_with_trials
        UNPIVOT(trial_end FOR transport IN (whatsap_trial, instagram_trial,avito_trial, vk_trial, telegram_trial, tgapi_trial ))
)
,trials_start_unpivot as (
        SELECT *except(transport),
        replace(transport,'_start','') as transport
        FROM accounts_with_trials
UNPIVOT(trial_start FOR transport IN (whatsap_trial_start, instagram_trial_start,avito_trial_start, vk_trial_start, telegram_trial_start, tgapi_trial_start   ))
),trials_unpivot as (
select trials_start_unpivot.account_id, 
       trials_start_unpivot.trial_start,
       trials_end_unpivot.trial_end,
       trials_start_unpivot.transport
from trials_start_unpivot
left join trials_end_unpivot on trials_end_unpivot.account_id = trials_start_unpivot.account_id 
                            and trials_end_unpivot.transport = trials_start_unpivot.transport 
),

all_trial_end_dates as (

select account_id , trial_start,trial_end, transport from trials_unpivot

union all 

select account_id, trial_end as trial_start, date_add(trial_end, interval 3 day), 'waba' as transport from channels_waba_trial)
,defining_min_trial_start as (
select account_id,
      min(trial_start) trial_start
from all_trial_end_dates
group by account_id)
select  -- Таблица аккаунтов с их датами старта и окончания триалов
      defining_min_trial_start.account_id,                                  -- ID аккаунта
       cast(defining_min_trial_start.trial_start as date) trial_start,      -- Дата начала триала
       cast(trial_end as date) trial_end                                    -- Дата окончания триала
from defining_min_trial_start
left join all_trial_end_dates on all_trial_end_dates.trial_start = defining_min_trial_start.trial_start
                                and all_trial_end_dates.account_id = defining_min_trial_start.account_id
group by defining_min_trial_start.account_id,
          cast(defining_min_trial_start.trial_start as date),
          cast(trial_end as date)