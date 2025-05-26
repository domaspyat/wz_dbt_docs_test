SELECT  id           --ID подписки
        , balance    --текущий баланс подписки
        , currency   --валюта подписки
        , created_at --дата и время создания подписки
        , deleted_at --дата и время удаления подписки
        , _ibk       --дата создания подписки
FROM `dwh-wazzup`.`wazzup`.`waba_subscription_gupshup`