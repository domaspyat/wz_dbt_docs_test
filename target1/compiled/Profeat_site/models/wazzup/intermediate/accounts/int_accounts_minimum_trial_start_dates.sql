with accounts_with_trials as (select account_id,
                                whatsap_trial_start,
                                instagram_trial_start,
                                avito_trial_start,
                                vk_trial_start,
                                telegram_trial_start,
                                tgapi_trial_start
                            from `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts`),

 channels_waba_trial as (
   select account_id,
  min(cast(datetime(created_at,'Europe/Moscow') as date))  as trial_start 
  from `dwh-wazzup`.`dbt_nbespalov`.`stg_channels` 
  where temporary=False and transport in ('waba','wapi')
  group by 1
 ),

trials_unpivot as (
SELECT * FROM accounts_with_trials
UNPIVOT(trial_start FOR transport IN (whatsap_trial_start, instagram_trial_start,avito_trial_start, vk_trial_start, telegram_trial_start, tgapi_trial_start ))),

all_trial_start_dates as (

select account_id , trial_start, transport from trials_unpivot

union all 

select account_id, trial_start, 'waba' as transport from channels_waba_trial)
    -- Таблица с датами триалов у аккаунтов
select account_id,                                                      -- ID аккаунта
 min(cast(trial_start as date)) as trial_start,                         -- Дата начала триала
  date_add(min(cast(trial_start as date)), interval 3 day) trial_end    -- Дата окончания триала
 from all_trial_start_dates
group by 1