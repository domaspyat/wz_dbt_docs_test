with payments_deduplicated as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_with_account_and_partner_type`
)
    -- Выручка, которую принес партнер в рублях (вносили либо дочки, либо партнер)
select 
        (case when account_type in ('partner','tech-partner')  then account_id
              when partner_type in ('partner','tech-partner') and account_type='standart' 
         then partner_id end) as partner_id,  -- аккаунт партнера
         paid_month,                          -- рассматриваемый месяц
    --count(distinct case when account_type in ('partner','tech-partner')  then account_id
                    --    when partner_type in ('partner','tech-partner') and account_type='standart' 
                  -- then account_id end) distinct_users_count,
    sum(case when account_type in ('partner','tech-partner') then sum_in_rubles end) as sum_in_rubles_partner_paid, -- выручка в рублях от партнера
    sum(sum_in_rubles) as sum_in_rubles       -- общая выручка в рублях
    from payments_deduplicated
    group by 1,2