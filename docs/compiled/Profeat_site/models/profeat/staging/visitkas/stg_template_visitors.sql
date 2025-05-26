SELECT 
    localUserId, 
    regexp_extract(url,r'https:\/\/([\w-]+).') as url,
    datetime as datetime,
    date(datetime) as date,
    cmuserid
FROM `dwh-wazzup`.`mongo_db`.`df_events`
where event='visitka-enter'