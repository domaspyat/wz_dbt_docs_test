SELECT    -- Информация с яндекс директа
          cast(date AS date) date                       -- Рассматриваемая дата
        , CAST(campaignid AS STRING) AS utm_campaign    -- Извлечение UTM campaign из URL, по которому зарегистрировался клиент
        , CampaignName as campaign_name                 -- Название рекламы
        , impressions                                   -- Количество показов
        , clicks                                        -- Количество кликов
        , COST                                          -- Стоимость
FROM `dwh-wazzup`.`wazzup`.`yandex_direct`