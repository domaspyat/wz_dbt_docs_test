select distinct localUserId, 
        date(dateTime) as event_date, 
        (case when url like '%?r=%' then url else null end) as url /* для определения реферального трафака */, 
        utm.utm_campaign, 
        utm.utm_source,
        utm.utm_medium,
        initReferrer,
        device,
        os,
        row_number() over (partition by localUserId order by dateTime asc) as rn
        from `dwh-wazzup`.`dbt_nbespalov`.`stg_all_data_filtered_from_test`
        where ((event in ('register-enter','sign-in-enter','landing.unique-visit')) or (event like '%business-select%') or (event like '%editor.add%'))