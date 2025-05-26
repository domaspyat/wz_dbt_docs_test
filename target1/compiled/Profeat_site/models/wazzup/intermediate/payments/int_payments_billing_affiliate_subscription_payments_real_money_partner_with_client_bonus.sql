with billing_affiliate_with_real_money as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_billing_affiliate_subscription_payments_real_money_with_who_paid`
),
balance_spending_partner AS (
                                 SELECT account_id                                                    AS partner_id
                                      , billing_affiliate_original_sum                                AS billing_affiliate_original_sum_paid_by_partner
                                      , subscriptionupdates_original_sum
                                      , wapi_original_sum
                                      , subscription_owner                                            AS account_id
                                      , billingaffiliate_currency                                     AS billingaffiliate_currency_partner
                                      , subscription_updates_currency                                 AS subscription_updates_currency_partner
                                      , occured_at                                                    AS paid_at -- (04.03.2025) 
                                      , occured_date                                                  AS paid_date
                                      , subscription_id
                                      , subscription_update_id
                                      , action
                                      , partner_discount                                              AS partner_discount
                                      , good_balance_spent                                            AS good_balance_spent
                                      , wapi_transactions_in_rubles                                   AS wapi_transactions_in_rubles
                                      , sum_in_rubles_full_subscription                               AS sum_in_rubles_full_subscription
                                      , sum_in_rubles_full_subscription - wapi_transactions_in_rubles AS subscription_sum
                                      , subscriptionupdates_original_sum - wapi_original_sum          AS subscription_sum_original_sum
                                      , sum_in_rubles
                                      , account_type
                                 FROM billing_affiliate_with_real_money
                                 WHERE has_partner_paid = 1
),
balance_spending_client AS (
                                SELECT account_id                                                         AS partner_id
                                     , subscription_owner                                                 AS account_id
                                     , occured_at                                                         AS paid_at -- (04.03.2025) 
                                     , occured_date                                                       AS paid_date
                                     , subscription_id
                                     , subscription_update_id
                                     , action
                                     , billing_affiliate_original_sum                                     AS billing_affiliate_original_sum_paid_by_client
                                     , partner_discount                                                   AS partner_discount
                                     , good_balance_spent                                                 AS good_balance_spent
                                     , coalesce(billingaffiliate_currency, subscription_updates_currency) AS billingaffiliate_currency_client
                                     , subscription_updates_currency                                      AS subscription_updates_currency_client
                                     , wapi_transactions_in_rubles                                        AS wapi_transactions_in_rubles
                                     , sum_in_rubles_full_subscription                                    AS sum_in_rubles_full_subscription
                                     , sum_in_rubles_full_subscription - wapi_transactions_in_rubles      AS subscription_sum
                                     , sum_in_rubles
                                FROM billing_affiliate_with_real_money
                                WHERE has_partner_paid = 0
                                ),
parnter_and_client_balance_with_wapi_discount as (
    select balance_spending_partner.subscription_update_id,         -- ID изменения, соответствует guid из subscriptionUpdates
    balance_spending_partner.partner_id,        -- ID партнера
    coalesce(balance_spending_client.account_id, balance_spending_partner.account_id)  as client_id,    -- ID клиента
    balance_spending_partner.partner_discount as partner_discount,  -- Скидка партнера
    balance_spending_partner.paid_at,           -- Дата и время оплаты (04.03.2025) 
    balance_spending_partner.paid_date,         -- Дата оплаты
    balance_spending_partner.subscription_id,   -- ID подписки
    balance_spending_partner.action,            -- Действие с подпиской
    coalesce(balance_spending_partner.good_balance_spent,0) as good_balance_spent_by_partner,   -- Кол-во хороших бонусов, потраченных партнером
    coalesce(balance_spending_client.good_balance_spent,0) as good_balance_spent_by_client,     -- Кол-во хороших бонусов, потраченных клиентом
    balance_spending_partner.sum_in_rubles as balance_spent_by_partner, -- Кол-во бонусов, потраченных партнером
    balance_spending_client.sum_in_rubles as balance_spent_by_client,   -- Кол-во бонусов, потраченных клиентом
    balance_spending_partner.wapi_transactions_in_rubles,       -- Сумма оплаты WABA баланса в рублях
    balance_spending_partner.sum_in_rubles_full_subscription,   -- Полная сумма подписки в рублях
    balance_spending_partner.subscription_sum,                  -- Сумма оплаты подписки
    billing_affiliate_original_sum_paid_by_client,              -- Сумма, оплаченная клиентом из billingAffiliate
    billing_affiliate_original_sum_paid_by_partner,             -- Сумма, оплаченная партнером из billingAffiliate
    billingaffiliate_currency_client,       -- Валюта клиента из billingAffiliate
    billingaffiliate_currency_partner,      -- Валюта партнера из billingAffiliate
    coalesce(subscription_updates_currency_client,subscription_updates_currency_partner) as subscription_updates_currency_client,   -- Валюта клиента из subscriptionUpdates
    subscription_updates_currency_partner,  -- Валюта партнера из subscriptionUpdates
    subscriptionupdates_original_sum,       -- Сумма подписки из subscriptionUpdates
    wapi_original_sum,                      -- Сумма пополнения баланса WABA
    account_type,                           -- Тип аккаунта
    subscription_sum_original_sum,          -- Сумма оплаты подписки
    (
        case when subscription_sum_original_sum=0 then 0
             when balance_spending_partner.subscription_sum_original_sum>=coalesce(balance_spending_client.billing_affiliate_original_sum_paid_by_client) then balance_spending_partner.subscription_sum_original_sum-coalesce(balance_spending_client.billing_affiliate_original_sum_paid_by_client,0)
             when balance_spending_partner.subscription_sum_original_sum <coalesce(balance_spending_client.billing_affiliate_original_sum_paid_by_client) then 0  --вся сумма ушла на пополнение баланса вабы
        else balance_spending_partner.subscription_sum_original_sum
        end
        ) as subscription_sum_without_balance_spent_by_client_original, --Стоимость подписки за вычетом бонусов, потраченных клиентом в валюте
    (
        case 
        when balance_spending_partner.subscription_sum=0 then 0
        when balance_spending_partner.subscription_sum>=coalesce(balance_spending_client.sum_in_rubles,0) then balance_spending_partner.subscription_sum-coalesce(balance_spending_client.sum_in_rubles,0)
        when balance_spending_client.sum_in_rubles>balance_spending_partner.subscription_sum then 0
        end
        ) as subscription_sum_withtout_balance_spent_by_client, --Стоимость подписки за вычетом бонусов, потраченных клиентом в рублях
    (
        case 
        when balance_spending_partner.subscription_sum=0 then 0
        when balance_spending_client.billing_affiliate_original_sum_paid_by_client is null then 0
        when balance_spending_partner.subscription_sum>=balance_spending_client.sum_in_rubles then balance_spending_client.sum_in_rubles
        else balance_spending_partner.subscription_sum
        end
        ) as balance_spent_by_client_on_subscription, --Сколько бонусов было потрачено клиентом ТОЛЬКО на покупку подписки (в рублях)
    (
        case 
        when balance_spending_partner.subscription_sum_original_sum=0 then 0
        when balance_spending_client.billing_affiliate_original_sum_paid_by_client is null then 0
        when balance_spending_partner.subscription_sum_original_sum>=balance_spending_client.billing_affiliate_original_sum_paid_by_client then balance_spending_client.billing_affiliate_original_sum_paid_by_client
        else balance_spending_partner.subscription_sum_original_sum 
        end
    ) as balance_spent_by_client_on_subscription_original, --Сколько бонусов было потрачено клиентом ТОЛЬКО на покупку подписки (в валюте)

    billing_affiliate_original_sum_paid_by_client-(
        case 
        when balance_spending_partner.subscription_sum_original_sum=0 then 0
        when balance_spending_client.billing_affiliate_original_sum_paid_by_client is null then 0
        when balance_spending_partner.subscription_sum_original_sum>=balance_spending_client.billing_affiliate_original_sum_paid_by_client then balance_spending_client.billing_affiliate_original_sum_paid_by_client
        else balance_spending_partner.subscription_sum_original_sum
        end
        ) as balance_spent_by_client_on_wapi_balance_original, --Сколько бонусов было потрачено клиентом ТОЛЬКО на пополнение баланса ВАБЫ (в валюте)

    wapi_original_sum-coalesce(
        billing_affiliate_original_sum_paid_by_client-(case 
        when balance_spending_partner.subscription_sum_original_sum=0 then 0
        when balance_spending_client.billing_affiliate_original_sum_paid_by_client is null then 0
        when balance_spending_partner.subscription_sum_original_sum>=balance_spending_client.billing_affiliate_original_sum_paid_by_client then balance_spending_client.billing_affiliate_original_sum_paid_by_client
        else balance_spending_partner.subscription_sum_original_sum
        end
        ),0 ) as wapi_subscription_to_pay_without_client_balance_original --Сколько было потрачено на пополнение баланса ВАБЫ без учета бонусов клиента (в валюте)

    from balance_spending_partner left join balance_spending_client 
    on balance_spending_partner.subscription_update_id=balance_spending_client.subscription_update_id 
    ),
    balance_and_payments as (
        select *,  
        (
            case when partner_discount is not null and ((paid_date>='2022-11-29'  and account_type='partner') or (paid_date>='2023-02-10' and account_type='tech-partner') or (partner_id=55875354)) 
            then 0.1*coalesce(wapi_subscription_to_pay_without_client_balance_original,0)
            else 0 end) as wapi_discount_for_partners_original, -- Комиссия партнера за пополнение баланса WABA
            ceil(subscription_sum_without_balance_spent_by_client_original*(1- coalesce(partner_discount,0))) as subscripion_sum_with_discount_original  -- Цена подписки со скидкой
            from parnter_and_client_balance_with_wapi_discount
        ),
        balance_and_payments_with_wapi_balance as (
            select *,
            ceil(wapi_subscription_to_pay_without_client_balance_original-coalesce(wapi_discount_for_partners_original,0)) as wapi_balance_partner_to_pay_original, -- Сумма, которую партнер должен заплатить за баланс WABA
            ceil(subscripion_sum_with_discount_original+wapi_subscription_to_pay_without_client_balance_original-coalesce(wapi_discount_for_partners_original,0)) as subscription_and_balance_partner_to_pay_original   -- Сумма за подписку + баланс WABA, которую должен заплатить партнер
            from balance_and_payments
),
/*

В СТЕ subscription_and_pay_with_converted_currency мы рассчитываем суммы, которые идут на пополнение вабы + на оплату подписок. 
Если валюта партнера  = валюте клиента И валюта клиента != рубли, мы просто переводим в рубли , умножая на курс. 
Если валюта партнера != валюте клиента И валюта клиента != рубли, то мы умножаем на курс + на комиссию (3%) за конвертацию.
Если валюта партнера != валюте клиента И валюта клиента = рубли, то мы просто умножаем на комиссию (3%) за конвертацию.
Если валюта партнера = валюте клиента  И валюта клиента рубли, то мы просто берем as is.
billingaffiliate_currency_client is null в случаях, когда клиент не использовал свои бонусы, платил только партнер

*/
    subscription_and_pay_with_converted_currency as (
        select balance_and_payments_with_wapi_balance.*,
            (
                case  
                when subscripion_sum_with_discount_original=0 then balance_spent_by_partner -- в рублях
                when billingaffiliate_currency_client is null and billingaffiliate_currency_partner='RUR' and billingaffiliate_currency_partner=subscription_updates_currency_client  then  balance_and_payments_with_wapi_balance.wapi_balance_partner_to_pay_original
                when billingaffiliate_currency_client is null and billingaffiliate_currency_partner=subscription_updates_currency_client   then exchange_rates_unpivoted_for_partner.rur*balance_and_payments_with_wapi_balance.wapi_balance_partner_to_pay_original
                when billingaffiliate_currency_client is null and billingaffiliate_currency_partner!=subscription_updates_currency_client and subscription_updates_currency_client='RUR'  then  balance_and_payments_with_wapi_balance.wapi_balance_partner_to_pay_original*1.03
                when billingaffiliate_currency_client is null and billingaffiliate_currency_partner!=subscription_updates_currency_client then  balance_and_payments_with_wapi_balance.wapi_balance_partner_to_pay_original*exchange_rates_unpivoted_client.rur*1.03
                when balance_and_payments_with_wapi_balance.subscription_updates_currency_client = 'RUR' and billingaffiliate_currency_client!=billingaffiliate_currency_partner then balance_and_payments_with_wapi_balance.wapi_balance_partner_to_pay_original*1.03
                when balance_and_payments_with_wapi_balance.subscription_updates_currency_client = 'RUR' then balance_and_payments_with_wapi_balance.wapi_balance_partner_to_pay_original
                when exchange_rates_unpivoted_client.rur is not null and billingaffiliate_currency_client!=billingaffiliate_currency_partner then (balance_and_payments_with_wapi_balance.wapi_balance_partner_to_pay_original) * (exchange_rates_unpivoted_client.rur*1.03)
                when exchange_rates_unpivoted_client.rur is not null and billingaffiliate_currency_client=billingaffiliate_currency_partner then (balance_and_payments_with_wapi_balance.wapi_balance_partner_to_pay_original) * (exchange_rates_unpivoted_client.rur)
                end
            ) as wapi_balance_partner_to_pay_sum_in_rubles, -- Сумма, которую партнер должен заплатить за баланс WABA в рублях
            (
                case
                when billingaffiliate_currency_partner='RUR' and  billingaffiliate_currency_client is null and   wapi_original_sum=0 then billing_affiliate_original_sum_paid_by_partner
                when wapi_original_sum=0 and billingaffiliate_currency_client is null  then balance_spent_by_partner
                when billingaffiliate_currency_client is null and  billingaffiliate_currency_partner='RUR' and billingaffiliate_currency_partner=subscription_updates_currency_client  then  balance_and_payments_with_wapi_balance.subscription_and_balance_partner_to_pay_original
                when billingaffiliate_currency_client is null 
                and billingaffiliate_currency_partner=subscription_updates_currency_client then 
                exchange_rates_unpivoted_for_partner.rur*balance_and_payments_with_wapi_balance.subscription_and_balance_partner_to_pay_original
                when billingaffiliate_currency_client is null  and billingaffiliate_currency_partner!=subscription_updates_currency_client and subscription_updates_currency_client='RUR'  then 
                balance_and_payments_with_wapi_balance.subscription_and_balance_partner_to_pay_original*1.03
                when billingaffiliate_currency_client is null  and billingaffiliate_currency_partner!=subscription_updates_currency_client   then 
                balance_and_payments_with_wapi_balance.subscription_and_balance_partner_to_pay_original*exchange_rates_unpivoted_client.rur*1.03
                when balance_and_payments_with_wapi_balance.subscription_updates_currency_client = 'RUR' and billingaffiliate_currency_client!=billingaffiliate_currency_partner
                then balance_and_payments_with_wapi_balance.subscription_and_balance_partner_to_pay_original*1.03
                when balance_and_payments_with_wapi_balance.subscription_updates_currency_client = 'RUR'
                then balance_and_payments_with_wapi_balance.subscription_and_balance_partner_to_pay_original
                when exchange_rates_unpivoted_client.rur is not null and billingaffiliate_currency_client!=billingaffiliate_currency_partner
                then (balance_and_payments_with_wapi_balance.subscription_and_balance_partner_to_pay_original) * (exchange_rates_unpivoted_client.rur*1.03)
                when exchange_rates_unpivoted_client.rur is not null and billingaffiliate_currency_client=billingaffiliate_currency_partner
                then (balance_and_payments_with_wapi_balance.subscription_and_balance_partner_to_pay_original) * (exchange_rates_unpivoted_client.rur)
                end
            ) as subscription_and_balance_partner_to_pay_sum_in_rubles, -- Сумма за подписку + баланс WABA, которую должен заплатить партнер в рублях
        exchange_rates_unpivoted_client.rur as client_currency,         -- Курс обмена валюты клиента
        exchange_rates_unpivoted_for_partner.rur as partner_currency,   -- Курс обмена валюты партнера
            (
                case
                when wapi_original_sum=0 then balance_spent_by_partner
                when billingaffiliate_currency_partner='RUR' and wapi_original_sum=0  and  billingaffiliate_currency_client is null  then billing_affiliate_original_sum_paid_by_partner
                when billingaffiliate_currency_client is null and  billingaffiliate_currency_partner='RUR' and billingaffiliate_currency_partner=subscription_updates_currency_client  then  balance_and_payments_with_wapi_balance.subscripion_sum_with_discount_original
                when billingaffiliate_currency_client is null and billingaffiliate_currency_partner=subscription_updates_currency_client then exchange_rates_unpivoted_for_partner.rur*balance_and_payments_with_wapi_balance.subscripion_sum_with_discount_original
                when billingaffiliate_currency_client is null  and billingaffiliate_currency_partner!=subscription_updates_currency_client and subscription_updates_currency_client='RUR'  then balance_and_payments_with_wapi_balance.subscripion_sum_with_discount_original*1.03
                when billingaffiliate_currency_client is null  and billingaffiliate_currency_partner!=subscription_updates_currency_client and subscription_updates_currency_partner!='RUR' then balance_and_payments_with_wapi_balance.subscripion_sum_with_discount_original*exchange_rates_unpivoted_client.rur*1.03
                when balance_and_payments_with_wapi_balance.subscription_updates_currency_client = 'RUR' and billingaffiliate_currency_client!=billingaffiliate_currency_partner then balance_and_payments_with_wapi_balance.subscripion_sum_with_discount_original*1.03
                when balance_and_payments_with_wapi_balance.subscription_updates_currency_client = 'RUR' then balance_and_payments_with_wapi_balance.subscripion_sum_with_discount_original
                when exchange_rates_unpivoted_client.rur is not null and billingaffiliate_currency_client!=billingaffiliate_currency_partner then (balance_and_payments_with_wapi_balance.subscripion_sum_with_discount_original) * (exchange_rates_unpivoted_client.rur*1.03)
                when exchange_rates_unpivoted_client.rur is not null and billingaffiliate_currency_client=billingaffiliate_currency_partner then (balance_and_payments_with_wapi_balance.subscripion_sum_with_discount_original) * (exchange_rates_unpivoted_client.rur)
                end
            ) as subscripion_sum_with_discount_converted_to_rubles,     -- Цена подписки со скидкой в рублях
            (
                case 
                when balance_spent_by_client_on_wapi_balance_original =0 then 0 
                when billingaffiliate_currency_client='RUR' then balance_spent_by_client_on_wapi_balance_original
                else coalesce(balance_spent_by_client_on_wapi_balance_original*exchange_rates_unpivoted_client.rur,0)               
                end
            ) as balance_spent_by_client_on_wapi_balance_sum_in_rubles, -- Сумма бонусов, потраченных клиентом на баланс WABA в рублях
            (case when subscription_updates_currency_client='RUR' then wapi_discount_for_partners_original
            else wapi_discount_for_partners_original*exchange_rates_unpivoted_client.rur
            end
            ) as wapi_discount_for_partners_sum_in_rubles               -- Комиссия партнера за пополнение баланса WABA в рублях

        from balance_and_payments_with_wapi_balance
        left join
            `dwh-wazzup`.`dbt_nbespalov`.`int_payments_exchange_rates_unpivoted` exchange_rates_unpivoted_client
            on exchange_rates_unpivoted_client._ibk = balance_and_payments_with_wapi_balance.paid_date
            and exchange_rates_unpivoted_client.currency = balance_and_payments_with_wapi_balance.subscription_updates_currency_client
        left join
            `dwh-wazzup`.`dbt_nbespalov`.`int_payments_exchange_rates_unpivoted` exchange_rates_unpivoted_for_partner
            on exchange_rates_unpivoted_for_partner._ibk = balance_and_payments_with_wapi_balance.paid_date
            and exchange_rates_unpivoted_for_partner.currency = balance_and_payments_with_wapi_balance.billingaffiliate_currency_partner),

subscription_and_pay_with_converted_currency_and_good_balance as (
    select *, 
    (
        case when good_balance_spent_by_client=0 then 0 
        when good_balance_spent_by_client>=balance_spent_by_client_on_subscription then balance_spent_by_client_on_subscription
        else good_balance_spent_by_client
        end
    ) as good_balance_spent_by_client_on_subscription,          -- Сумма хороших бонусов, потраченных клиентом на подписку
    good_balance_spent_by_client-(
        case when good_balance_spent_by_client=0 then 0 
        when good_balance_spent_by_client>=balance_spent_by_client_on_subscription then balance_spent_by_client_on_subscription
        else good_balance_spent_by_client
        end
    ) as good_balance_spent_by_client_on_waba_balance,          -- Сумма хороших бонусов, потраченных клиентом на баланс WABA
    (
        case 
        when wapi_original_sum=0 and good_balance_spent_by_partner>=subscripion_sum_with_discount_converted_to_rubles then good_balance_spent_by_partner
        when good_balance_spent_by_partner>=subscripion_sum_with_discount_converted_to_rubles then subscripion_sum_with_discount_converted_to_rubles
        else good_balance_spent_by_partner
        end) as good_balance_spent_by_partner_on_subscription,  -- Сумма хороших бонусов, потраченных партнером на подписку
    good_balance_spent_by_partner-
    (
        case 
        when wapi_original_sum=0 and good_balance_spent_by_partner>=subscripion_sum_with_discount_converted_to_rubles then good_balance_spent_by_partner
        when good_balance_spent_by_partner>=subscripion_sum_with_discount_converted_to_rubles then subscripion_sum_with_discount_converted_to_rubles
        else good_balance_spent_by_partner
        end) as good_balance_spent_by_partner_on_waba_balance   -- Сумма хороших бонусов, потраченных партнером на баланс WABA
    from subscription_and_pay_with_converted_currency),
subscription_and_pay_with_converted_currency_and_good_and_bad_balance as (
    select *, round(balance_spent_by_client_on_subscription,2)-round(good_balance_spent_by_client_on_subscription,2) as bad_balance_spent_by_client_on_subscription_sum_in_rubles,              -- Сумма плохих бонусов, потраченных клиентом на подписку в рублях
              round(balance_spent_by_client_on_wapi_balance_sum_in_rubles,2)-round(good_balance_spent_by_client_on_waba_balance,2) as bad_balance_spent_by_client_on_waba_balance_sum_in_rubles,-- Сумма плохих бонусов, потраченных клиентом на баланс WABA в рублях
              round(subscripion_sum_with_discount_converted_to_rubles,2)-round(good_balance_spent_by_partner_on_subscription,2) as bad_balance_spent_by_partner_on_subscrpition_sum_in_rubles,  -- Сумма плохих бонусов, потраченных партнером на подписку в рублях
              round(wapi_balance_partner_to_pay_sum_in_rubles,2)-round(good_balance_spent_by_partner_on_waba_balance,2) as bad_balance_spent_by_partner_on_waba_balance_sum_in_rubles           -- Сумма плохих бонусов, потраченных партнером на баланс WABA в рублях
    from subscription_and_pay_with_converted_currency_and_good_balance),

subscription_and_pay_with_converted_currency_and_good_and_bad_balance_partner_and_client as (    
select *, good_balance_spent_by_client_on_waba_balance+good_balance_spent_by_partner_on_waba_balance as sum_in_rubles_spent_on_waba_balance,                -- Сумма, потраченная на баланс WABA в рублях
          good_balance_spent_by_client_on_subscription+good_balance_spent_by_partner_on_subscription as sum_in_rubles_spent_on_subscription,                          -- Сумма, потраченная на подписку в рублях
          bad_balance_spent_by_client_on_waba_balance_sum_in_rubles+bad_balance_spent_by_partner_on_waba_balance_sum_in_rubles as bad_balance_spent_on_waba_balance,   -- Сумма плохих бонусов, потраченных на баланс WABA
          bad_balance_spent_by_partner_on_subscrpition_sum_in_rubles as bad_balance_spent_on_subscription
 from subscription_and_pay_with_converted_currency_and_good_and_bad_balance)
        -- Таблица платежей, которая отражает количество реально потраченных денег партнером при оплате подписок клиента

 select * from subscription_and_pay_with_converted_currency_and_good_and_bad_balance_partner_and_client 
 /*
 Реальные деньги - реальные деньги
Бонусы:
- Хорошие:
1) Некорректный счет
2) Перевод с другого аккаунта
3) Пополнение партнерского счета реальными деньгами
- Плохие:
все остальные

Хороший баланс = реальные деньги + хорошие бонусы
Плохой баланс = 
*/