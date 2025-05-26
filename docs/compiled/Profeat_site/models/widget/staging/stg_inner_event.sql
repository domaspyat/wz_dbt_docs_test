select id,
        _ibk,
        createdAt as created_at,
        userid as user_id,
        other_text,
        duration,
        sum,
        order_id,
        details,
        name
from `dwh-wazzup`.`widget`.`inner_event`