with all_trial_dates as (
            select *,
                   row_number() over (partition by account_id order by date) rn
            from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_minimum_trial_start_dates` starts
            join `dwh-wazzup`.`dbt_nbespalov`.`stg_days`  days on starts.trial_start<= days.date and  days.date <=  starts.trial_end
),

trials_at_the_moment as (
            select account_id,
                    rn as trial_at_the_moment,
                    date as trial_date_at_the_moment
            from all_trial_dates
            where date = current_date()
)
    -- Таблица с датами окончания триалов
select distinct all_trial_dates.account_id,
     case when trials_at_the_moment.account_id is null 
            then max(all_trial_dates.date) over (partition by all_trial_dates.account_id)
     when trial_end >= current_date() 
             then trial_date_at_the_moment
     else max(trial_date_at_the_moment) over (partition by all_trial_dates.account_id) 
     end as trial_max_date_at_the_moment,               -- Максимальная дата триала сейчас

     case when trial_end >= current_date() then cast(trial_at_the_moment as string)
             else 'trial_ended'
     end trial_max_day,                                 -- Если триал еще не закончился, то партиция по account_id order by date
from all_trial_dates
left join trials_at_the_moment on all_trial_dates.account_id = trials_at_the_moment.account_id