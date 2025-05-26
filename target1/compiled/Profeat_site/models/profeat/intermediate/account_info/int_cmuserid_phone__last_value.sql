with stg_cmuserid_phone as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_cmuserid_phone`
    ),

phones_by_users as (
    select * from stg_cmuserid_phone
    ),

phones_by_users_to_deduplicate as (
    select cmuserid,
    row_number() over (partition by cmuserid order by datetime desc) rn,
    phone
    from phones_by_users),
    
phones_by_users_deduplicated as
    (select * from phones_by_users_to_deduplicate
    where rn=1)


select cmuserid, phone from phones_by_users_deduplicated