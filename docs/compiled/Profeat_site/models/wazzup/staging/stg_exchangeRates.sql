select          -- Таблица - справочник с курсом валют на каждую дату
    data,               -- Дата
    currency,           -- Валюта
    _ibk,               -- Дата. _ibk необходимо для партицирования данных в BigQuery
    (case when _ibk='2024-03-14' and nominal='RUR' and currency='KZT' then 0.203750
        when _ibk='2024-03-14' and nominal='RUR' and currency='USD' then 91.5449    
        when _ibk='2024-03-14' and nominal='RUR' and currency='EUR' then 100.1869
        when _ibk='2024-03-15' and nominal='RUR' and currency='KZT' then 0.203685
        when _ibk='2024-03-15' and nominal='RUR' and currency='USD' then 91.6359
        when _ibk='2024-03-15' and nominal='RUR' and currency='EUR' then 100.2625
        when _ibk='2024-03-16' and nominal='RUR' and currency='KZT' then 0.204537
        when _ibk='2024-03-16' and nominal='RUR' and currency='USD' then 91.87
        when _ibk='2024-03-16' and nominal='RUR' and currency='EUR' then 99.9718
        when _ibk='2024-03-17' and nominal='RUR' and currency='KZT' then 0.204537
        when _ibk='2024-03-17' and nominal='RUR' and currency='USD' then 91.87
        when _ibk='2024-03-17' and nominal='RUR' and currency='EUR' then 99.9718
        when _ibk='2024-03-18' and nominal='RUR' and currency='KZT' then 0.204537
        when _ibk='2024-03-18' and nominal='RUR' and currency='USD' then 91.87
        when _ibk='2024-03-18' and nominal='RUR' and currency='EUR' then 99.9718
        else cor_rate -- У нас не менялся курс с  14 по 18 марта 2024 , из-за этого было расхождение с 1с https://wazzup.planfix.ru/task/1187688
    end) as cor_rate,           -- Курс валюты. Если на текущий день курс null, то берется последнее not null значение
    nominal                     -- Номинал валюты. Если, например currency = 'EUR', а nominal = 'RUR', это означает, что мы смотрим сколько стоит 1 евро на дату.
from `dwh-wazzup`.`wazzup`.`exchangeRates`