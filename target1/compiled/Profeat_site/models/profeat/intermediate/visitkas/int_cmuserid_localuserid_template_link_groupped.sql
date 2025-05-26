with template_link_by_user as (
    select
    cmuserid,
    localuserid,
    last_value(regexp_extract(NET.HOST(templatelink),r'[^.]+')) over (
        partition by cmuserid
        order by datetime range between unbounded preceding and unbounded following
    ) as last_templatelink
    from `dwh-wazzup`.`mongo_db`.`df_events`
    where event != 'visitka-enter' and templatelink is not null),

    template_link_by_user_deduplicated as (
        select cmuserid,localUserId,last_templateLink as template_link
        from template_link_by_user
        group by 1,2,3
    )
select * from template_link_by_user_deduplicated