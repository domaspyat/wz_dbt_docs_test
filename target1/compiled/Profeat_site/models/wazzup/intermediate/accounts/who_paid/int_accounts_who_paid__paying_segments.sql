with old_and_new_billing__merged as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_who_paid__old_and_new_billing__merged`
),

paying_segments as (
        select *,  
		(case when partner_id is null and refparent_id is null then 'standart' /*если у клиента нет ни партнера, ни реферала, то он 100% сам платит за подписку */
		 when partner_type='tech-partner' then 'tech-partner_child_account'
		 when payer_partner_account_id is null then 'standart' /* 100% это оплаты самих клиентов */
		 else 'partner_child_account' /*была оплата партнером */ 
		 end
		 ) as segment_type
		 from old_and_new_billing__merged
		 ),
final_paying_segments as (
        select *except(segment_type),
         case when payer_partner_account_id is null and segment_type = 'tech-partner_child_account' then 'tech-partner_child_account_independent'
              when payer_partner_account_id is not null and segment_type = 'tech-partner_child_account' then 'tech-partner_child_account_dependent'
              else segment_type
         end as segment_type    -- Сегмент клиента
from paying_segments)
select *    -- Таблица с аккаунтами и информацией о плательщике с его сегментом
from final_paying_segments