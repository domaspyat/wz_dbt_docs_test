with payer_segments as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_who_paid__paying_segments`
),

payer_segment_with_payer_accounts as ( 
 
    select payer_segments.*,
    (case when segment_type='standart' then account_id --если платил сам клиент, пишем его accountId 
    when activation_reason_id = '0c03af36-b95f-46d3-83a3-4163cc311530' then 15769389 --непонятный акк, какая-то бага в базе
    when payer_partner_account_id is not null then payer_partner_account_id
    else account_id --если платил партнер, пишем его accountId 
    end
    ) as payer_account  -- ID аккаунта плательщика
    from  payer_segments)
    -- Таблица с аккаунтами и информацией о плательщике с типами и ID аккаунтов
select * from payer_segment_with_payer_accounts