select id,
        name,
        settingsid as settings_id,
        dialogDelivered as dialog_delivered,
        deviceType as device_type,
        createdAt as created_at,
        _ibk,
        wazzupId as wazzup_id
from `dwh-wazzup`.`widget`.`event`