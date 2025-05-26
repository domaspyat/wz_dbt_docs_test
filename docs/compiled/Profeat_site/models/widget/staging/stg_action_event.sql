select id,
        actionid as action_id,
        settingsid as settings_id,
        name,
        updatedAt as updated_at,
        createdAt as created_at,
        _ibk
from `dwh-wazzup`.`widget`.`action_event`