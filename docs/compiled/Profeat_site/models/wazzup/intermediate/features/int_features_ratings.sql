-- Таблица c аккаунтами, рейтингом и отзывом, который клиенты оставили о Wazzup
select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_ratings`
where rn=1  -- Берется только последний отзыв на аккаунт