with int_funnel_key_events__counting_users_on_each_stage as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_funnel_key_events__counting_users_on_each_stage`
),

int_funnel_key_event__unpivot_events as (
    select * from int_funnel_key_events__counting_users_on_each_stage 
    unpivot (
            users for event in (SuccReg,TemplateUsage,Edits,Activation,Copies,Posted,paid,repeat_paid,first_paid)
            )
    )

select * from int_funnel_key_event__unpivot_events