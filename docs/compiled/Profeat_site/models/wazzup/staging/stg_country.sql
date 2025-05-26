select          -- Таблица - справочник со списком стран и кодами в стандарте ISO2
    iso2,                           -- Двухсимвольный код страны
    russianName as russian_name     -- Наименование страны на русском языке
from `dwh-wazzup`.`analytics_tech`.`country`