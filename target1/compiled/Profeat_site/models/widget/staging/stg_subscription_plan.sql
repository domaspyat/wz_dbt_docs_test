select id,
        planname as plan_name,
        features,
        duration,
        unit,
        _ibk,
        createdat as created_at,
        updatedat as updated_at,
        amount
from `dwh-wazzup`.`widget`.`subscription_plan`