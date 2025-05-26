select *, 
    replace(
        replace(replace(replace(usermobile, '+', ''), '-', ''), ')', ''), '(', ''
    ) as phone from `dwh-wazzup`.`mongo_db`.`df_events`