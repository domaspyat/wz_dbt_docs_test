select *
from `dwh-wazzup`.`dbt_nbespalov`.`int_funnel_key_events__finding_all_users_stages`
where eventgroupname_description in  ('Мультиссылка','Мобильный сайт','Универсальный шаблон')
order by eventgroupname_description