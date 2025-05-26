select id,
        _ibk,
        createdAt as created_at,
        updatedAt as updated_at,
        domain,
        widgetOwnerKey as widget_owner_key,
        referrer,
        widgetownerid as widget_owner_id,
        widgetsettingsid as widget_settings_id
from `dwh-wazzup`.`widget`.`statistics_setup_event`