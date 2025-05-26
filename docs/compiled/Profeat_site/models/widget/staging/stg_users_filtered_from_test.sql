with tests as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`widget_test_phones`
)
select id,
        username,
        telegram_username,
        phone,
        paymentMethodId as payment_method_id,
        utm_source,
        deviceTypes as device_types,
        utm_campaign,
        initRefferer,
        utm_term,
        utm_content,
        utm_medium,
        qualification,
        token,
        password,
        password_salt,
        email,
        telegram_profile,
        _ibk,
        createdat as created_at,
        cast(createdat as date) registration_date,
        updatedat as updated_at
from `dwh-wazzup`.`widget`.`user` user
where not exists (select username
                    from tests
                    where user.username = tests.username
                     )