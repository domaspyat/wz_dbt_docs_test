with key_reply_bills as 
(SELECT SPLIT(invoice_billing_month, ' ')[OFFSET(0)] as month, 
        SPLIT(invoice_billing_month, ' ')[OFFSET(1)] as year,* 
FROM `dwh-wazzup`.`analytics_tech`.`waba_sessions_and_subscription_key_reply`
)
    -- Таблица платежей WABA KeyReply
select PARSE_DATE('%m-%Y', concat(months.number, '-', year)) as paid_month, *
from key_reply_bills
left join `dwh-wazzup`.`analytics_tech`.`month_russian_names` months on key_reply_bills.month=months.russian_name
where year is not null