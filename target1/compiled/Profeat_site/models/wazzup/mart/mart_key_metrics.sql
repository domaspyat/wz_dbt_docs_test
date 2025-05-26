select mart.*,  -- Таблица с ключевыми метриками Wazzup
        int_accounts_first_subscription_date_and_type.tariff as int_accounts_first_subscription_date_and_type__tariff,      -- Тариф первой подписки
        int_accounts_first_subscription_date_and_type.period as int_accounts_first_subscription_date_and_type__period,      -- Период первой подписки
        int_accounts_first_subscription_date_and_type.quantity as int_accounts_first_subscription_date_and_type__quantity,  -- Кол-во каналов в первой подписке
        int_accounts_first_subscription_date_and_type.start_date,       -- Дата начала первой подписки
        int_accounts_first_subscription_date_and_type.subscription_type as int_accounts_first_subscription_date_and_type_subscription__type,    -- Тип первой подписки
        int_accounts_first_subscription_date_and_type.paid_at,          -- Дата и время оплаты первой подписки
        int_accounts_first_subscription_date_and_type.subscription_id,  -- ID первой подписки
        
        int_accounts_mobile_app_visited_at_first_month.account_Id as int_accounts_mobile_app_visited_at_first_month_account_id, -- ID аккаунта, который посетил мобильное приложение в первый месяц после регистрации


        int_channels_first_month_by_type.*except(account_Id),
        int_channels_whatsapp_paid_added_first_month.*except(account_Id),       -- Количество каналов WHATSAPP, добавленных в первый месяц после регистрации
        int_channes_unique_chats_per_accounts_first_month.*except(account_Id),  -- Количество уникальных диалогов в первый месяц после регистрации
        
        int_payments_revenue_vs_subscriptions_first_month.full_tarif_sum_in_rubles as int_payments_revenue_vs_subscriptions_first_month__full_tarif_sum_in_rubles,  -- Полная сумма тарифа в рублях
        int_payments_revenue_vs_subscriptions_first_month.sum_in_rubles as int_payments_revenue_vs_subscriptions_first_month__sum_in_rubles,    -- Сумма в рублях

        int_subscriptions_more_than_2_types_1_month_registration.*,
        int_subscriptions_sum_in_first_month.sum_in_rubles_by_period as int_subscriptions_sum_in_first_month__sum_in_rubles_by_period,  -- Сумма оплаты, разделенная на период подписки
        int_subscriptions_sum_in_first_month.sum_in_rubles as int_subscriptions_sum_in_first_month__sum_in_rubles,                      -- Сумма оплаты
       
        int_subscriptions_sum_three_month.sum_in_rubles_by_period as int_subscriptions_sum_three_month__sum_in_rubles_by_period,        -- Сумма оплаты, разделенная на период подписки
        int_subscriptions_sum_three_month.sum_in_rubles_by_period as int_subscriptions_sum_three_month__sum_in_rubles,                  -- Сумма оплаты, разделенная на период подписки
       
        mart_key_product_metrics__conversion_c2_1_month.*except(account_Id),
        mart_key_product_metrics__telegram_notification.*except(account_Id),
        profile_info.currency       -- Валюта

from `dwh-wazzup`.`dbt_nbespalov`.`mart_onboarding__accounts_integrations_subscriptions_channels_messages` mart
join `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info on mart.account_id = profile_info.account_id
left join `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_first_subscription_date_and_type` on mart.account_Id = int_accounts_first_subscription_date_and_type.account_Id
left join `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_mobile_app_visited_at_first_month` on mart.account_id = int_accounts_mobile_app_visited_at_first_month.account_id
left join `dwh-wazzup`.`dbt_nbespalov`.`int_channels_first_month_by_type` on mart.account_Id = int_channels_first_month_by_type.account_Id
left join `dwh-wazzup`.`dbt_nbespalov`.`int_channels_whatsapp_paid_added_first_month` on mart.account_id = int_channels_whatsapp_paid_added_first_month.account_id
left join `dwh-wazzup`.`dbt_nbespalov`.`int_channes_unique_chats_per_accounts_first_month` on mart.account_Id =  int_channes_unique_chats_per_accounts_first_month.account_Id
left join `dwh-wazzup`.`dbt_nbespalov`.`int_payments_revenue_vs_subscriptions_first_month` on mart.account_Id = int_payments_revenue_vs_subscriptions_first_month.account_Id
left join `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_more_than_2_types_1_month_registration` on mart.account_Id = int_subscriptions_more_than_2_types_1_month_registration.accounts_with_2_or_more_subscription_type
left join `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_sum_in_first_month` on mart.account_Id = int_subscriptions_sum_in_first_month.account_Id
left join `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_sum_three_month` on mart.account_Id = int_subscriptions_sum_three_month.account_Id
left join `dwh-wazzup`.`dbt_nbespalov`.`mart_key_product_metrics__conversion_c2_1_month` on mart.account_Id = mart_key_product_metrics__conversion_c2_1_month.account_Id
left join `dwh-wazzup`.`dbt_nbespalov`.`mart_key_product_metrics__telegram_notification` on mart.account_id = mart_key_product_metrics__telegram_notification.account_id