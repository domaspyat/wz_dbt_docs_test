with subscription_updates as (
    select * from 
    `dwh-wazzup`.`dbt_nbespalov`.`int_subscription_updates__corect_date_and_filledna_tarif_period_quantity`

)
    -- Таблица с аккаунтами и ID причины последнего изменения (не обещанный платеж)
select 
		distinct billingPackages.account_id,    -- ID аккаунта
		first_value((case when action ='setPromisedPayment' then null else activation_reason_id end) ignore nulls)
		over (partition by billingPackages.account_id order by subscription_updates.paid_at  desc  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
)
		as activation_reason_id                 -- тут док акт ризон ид
		from `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` billingPackages
		left join subscription_updates on billingPackages.guid=subscription_updates.subscription_id
		where billingPackages.state='active' 
		and (is_free=False or is_free is null) /* это не безвозмездно отданная подписка */