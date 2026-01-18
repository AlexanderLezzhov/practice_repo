WITH last_one_month_sale as ( 
SELECT
    p.itemid
    , p.name_ax
    , p.brand_name_ax
    , p.category
    , SUM(s.qty) item_qty
    , SUM(s.qty * s.price) item_revenue
    , SUM(s.qty * s.price) / SUM(s.qty) avg_item_price
FROM dwh.sales s
JOIN dwh.products p USING(itemid)
WHERE 
    1 = 1
    -- Последний полный месяц это всегда прошедший, так как пока текущий месяц не кончился, он технически и концептуально не может быть полным
    AND date_trunc('month',  s.date ) = date_trunc('month', now() ) - interval '1 month'
GROUP BY
     p.itemid
    , p.name_ax
    , p.brand_name_ax
    , p.category
),

/* продаж по itemid может не быть за какие-то ди интервала 3 месяцев
поэтому предполагаю, что есть таблица с календарём по дням и делаю такое решение, чтобы
если у itemid нет продаж за день, ставился 0 и участвовал в общей метрике */

items AS (
    SELECT 
        DISTINCT itemid
    FROM dwh.sales
    WHERE date_trunc('month', date) BETWEEN date_trunc('month', now()) - interval '3 month'
                                        AND date_trunc('month', now()) - interval '1 month'
),

last_three_month_sales AS (
SELECT
    i.itemid
    , AVG(item_qty_per_day ) AS avg_qty_per_day
FROM (
    SELECT
        i.itemid
        , c.day_date
        , COALESCE(SUM(s.qty),0 ) AS item_qty_per_day
    FROM items i
    CROSS JOIN (
        SELECT calendar_date AS day_date
        FROM calendar
        WHERE date_trunc('month', day_date) 
              BETWEEN date_trunc('month', now()) - interval '3 month'
                  AND date_trunc('month', now()) - interval '1 month'
    ) c
    LEFT JOIN dwh.sales s ON s.itemid = i.itemid AND date_trunc('day', s.date) = c.day_date
    GROUP BY 
        i.itemid
        , c.day_date
) t
GROUP BY 
    i.itemid
),

stock_curr_status as(
SELECT
    itemid
    , qty stock_qty
FROM(   
    SELECT
        itemid
        , qty
        , ROW_NUMBER() OVER(PARTITION BY itemid ORDER BY date DESC) rn
    FROM dwh.stock
    ) t
WHERE
    rn = 1
)

SELECT
    lm.*
    , ltm.avg_qty_per_day
    , st.stock_qty - ltm.avg_qty_per_day
FROM last_one_month_sale lm
LEFT JOIN last_three_month_sale ltm USING(itemid)
LEFT JOIN stock_curr_status st USING(itemid)





    