/*  Тестил на реальном клике, правильно считает интервал и стык между концом и началом года */

SELECT DISTINCT
    toDate(timestamp) AS dt
    , COUNT(DISTINCT user_id) OVER (ORDER BY toRelativeDayNum(toDate(timestamp) )
        RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS wau
FROM dwh.site_activity
WHERE toDate(timestamp)
ORDER BY dt DESC