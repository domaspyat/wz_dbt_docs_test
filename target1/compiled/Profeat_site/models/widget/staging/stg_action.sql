select id,
        enabled,
        query,
        userid as user_id,
        settingsid as settings_id,
        showOncePerVisit as show_once_per_visit,
        updatedAt as updated_at,
        createdAt as created_at,
        type,
        name,
        _ibk
from `dwh-wazzup`.`widget`.`action`