select id,
        _ibk,
        createdat as created_at,
        userid as user_id,
        subscriptionPlanid as subscription_plan_id,
        orderId as order_id,
        status
from `dwh-wazzup`.`widget`.`payment`