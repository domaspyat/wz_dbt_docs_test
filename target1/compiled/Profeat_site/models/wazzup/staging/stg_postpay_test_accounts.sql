SELECT          -- таблица с тестовыми аккаунтами партнеров постоплатников. Данные заполняются вручную в гугл шите https://docs.google.com/spreadsheets/d/1asPlgy2CWnaTFYgDN6YHsbd2qcah8KNmdBMAxf4UZIs/edit?gid=0#gid=0
    partner_id,             -- Id партнера
    test_account_id         -- Id тестового аккаунта
FROM `dwh-wazzup`.`google_sheets`.`postpay_test_account_scheduled`