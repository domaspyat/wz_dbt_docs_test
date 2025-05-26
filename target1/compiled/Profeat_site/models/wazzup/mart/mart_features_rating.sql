with ratings_text as (
  select account_id,
  _ibk as occured_date,
  rating,
  text from `dwh-wazzup`.`dbt_nbespalov`.`stg_ratings` ratings
  where rn=1

),

segments as (
    select account_id, 
    live_month,
    avg_sum_in_rubles,
    abcx_segment,
    last_value(avg_sum_in_rubles ignore nulls) over (partition by account_id order by live_month asc rows between unbounded preceding and current row) as last_value_avg_sum_in_rubles,
    last_value(abcx_segment ignore nulls) over (partition by account_id order by live_month asc rows between unbounded preceding and current row) as last_value_abcx_segment 
    from
   `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_abcx_segmentation_defining_abcx_segmentation_type_all_segments`
),

integrations_by_date as (
    select account_id,
    last_value_integration_type_month,
    date_trunc(date,month) as integration_month
     from `dwh-wazzup`.`dbt_nbespalov`.`mart_integrations_by_month_paying_users`
     group by 1,2,3
),


ratings_and_segments as (


SELECT coalesce(ratings_text.account_id, segments.account_id) as account_id,    -- ID аккаунта
  occured_date,                                         -- Дата события
  rating,                                               -- Рейтинг
  text ,                                                -- Текст отзыва
last_value(last_value_avg_sum_in_rubles ignore nulls) over (partition by coalesce(ratings_text.account_id, segments.account_id) order by occured_date asc rows between unbounded preceding and current row) as last_value_avg_sum_in_rubles,    -- Последняя средняя сумма покупки в рублях
last_value(last_value_abcx_segment ignore nulls) over (partition by coalesce(ratings_text.account_id, segments.account_id) order by occured_date asc rows between unbounded preceding and current row) as last_value_abcx_segment,  -- Последне значение по ABCX сегментации
profile_info.account_segment_type,                      -- Сегмент аккаунта
profile_info.phone,                                     -- Телефон, указанный при регистрации
profile_info.email,                                     -- Почта, указанная при регистрации
profile_info.currency,                                  -- Валюта
profile_info.russian_country_name,                      -- Название страны на русском языке
profile_info.register_date,                             -- Дата регистрации
profile_info.region_international,                      -- Регион
profile_info.first_paid_subscription_date_with_1_more_day_duration, -- Дата первой оплаченной подписки с продолжительностью больше 1 дня
profile_info.account_language,                          -- Язык аккаунта
profile_info.active_integration_name,                   -- Название активной интеграции
segments.avg_sum_in_rubles,                             -- Средняя сумма покупки в рублях
segments.abcx_segment,                                  -- ABCX сегмент
integrations_by_date.last_value_integration_type_month  -- Последний тип интеграции
FROM segments

full outer join ratings_text on segments.account_id=ratings_text.account_id 
and segments.live_month=date_trunc(ratings_text.occured_date,month)
left join `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info
on coalesce(ratings_text.account_id, segments.account_id)=profile_info.account_id
left join integrations_by_date
on integrations_by_date.account_id=coalesce(ratings_text.account_id, segments.account_id)
and date_trunc(ratings_text.occured_date,month)=integrations_by_date.integration_month
where not is_employee)
    -- Таблица с рейтингом фич
select * from ratings_and_segments
where occured_date is not null