select id,
        whatsappNumber as whatsapp_number,
        whatsappGreetingMessage as whatsapp_greeting_message,
        userId as user_id,
        _ibk,
        createdat as created_at,
        updatedAt as updated_at,
        appendWazzupId as append_wazzup_id,
        whatsappenabled as whatsapp_enabled,
        telegramEnabled as telegram_enabled,
        telegramusername as telegram_username,
        name,
        hidden
from `dwh-wazzup`.`widget`.`settings`