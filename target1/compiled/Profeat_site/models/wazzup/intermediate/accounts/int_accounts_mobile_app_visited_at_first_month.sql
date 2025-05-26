with
    mobile_ab_min_visited_date as (
        select
            accountid as account_id,
            min(cast(visitedat as date)) as visited_at_first_date
        from dwh-wazzup.snapshots.crmEmployees_snapshot
        group by 1
    ),

    visit_at_first_month as (
        select mobile_ab_min_visited_date.account_id    -- ID аккаунта
        from mobile_ab_min_visited_date
        inner join
            `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info
            on mobile_ab_min_visited_date.account_id = profile_info.account_id
        where mobile_ab_min_visited_date.visited_at_first_date<=date_add(profile_info.register_date, interval 1 month)
    )
    -- Таблица с аккаунтами, которые зашли в мобильное приложение в первый месяц после регистрации
select * from visit_at_first_month