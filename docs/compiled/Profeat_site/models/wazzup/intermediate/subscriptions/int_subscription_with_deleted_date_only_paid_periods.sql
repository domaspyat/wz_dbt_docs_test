with subscription_only_paid as (
select int_subscriptions_with_deleted_date.* from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_who_paid_restore_missing_data__paidat_and_expiresat_deduplicated` restore_missing_data
inner join `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_with_deleted_date` int_subscriptions_with_deleted_date
on restore_missing_data.subscription_id=int_subscriptions_with_deleted_date.subscription_id 
and restore_missing_data.start_date=int_subscriptions_with_deleted_date.start_date
where action is distinct from 'setPromisedPayment'
)
    -- Таблица подписок, у которых есть дата удаления и оплаченный период
select * from subscription_only_paid