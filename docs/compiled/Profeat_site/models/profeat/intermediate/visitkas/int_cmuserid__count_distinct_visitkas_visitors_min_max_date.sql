SELECT
    MIN(Date) AS min_date,
    MAX(Date) AS max_date
  FROM
    `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid__count_distinct_visitkas_visitors`