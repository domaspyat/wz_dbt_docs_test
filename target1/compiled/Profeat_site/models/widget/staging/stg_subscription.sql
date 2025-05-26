select id,
        paymentDate as payment_date,
        _ibk,
        dueDate as due_date,
        userid as user_id,
        subscriptionPlanid as subscription_plan_id
from `dwh-wazzup`.`widget`.`subscription`