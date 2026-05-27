/* Создание (временной) таблицы sales + id промопериода */

DROP TABLE IF EXISTS sales_w_promo_periods;
CREATE TEMPORARY TABLE sales_w_promo_periods AS
WITH stores_w_promo_pre AS (
SELECT 
	*
	, period_id - ROW_NUMBER() OVER(PARTITION BY store_id ORDER BY period_id) constant_period_id
FROM public.sales
)
SELECT
	store_id
	, period_id
	, sales_volume
	, sale_id
	, store_id::text || '_' 
		|| MIN(period_id) OVER(PARTITION BY store_id, constant_period_id)::text || '_'
		|| MAX(period_id) OVER(PARTITION BY store_id, constant_period_id)::text promo_period_id 
-- ID промопериода в формате <id магазина>_<период начала>_<период окончания>
FROM stores_w_promo_pre ;

DROP TABLE IF EXISTS top_five_stores;
CREATE TEMPORARY TABLE top_five_stores AS
WITH store_sales AS(
SELECT
	t.type_name
	, s.store_id
	, SUM(s.sales_volume) sales_volume
FROM public.sales s
JOIN public.store_chars c USING(store_id)
JOIN public.store_types t ON c.store_type_id = t.type_id
GROUP BY 
	t.type_name
	, s.store_id
),
ranked_sales AS(
SELECT 
	*
	, RANK() OVER(PARTITION BY type_name ORDER BY sales_volume DESC ) rn
FROM store_sales
)
SELECT 
	type_name
	, store_id
FROM ranked_sales
WHERE 
	rn <= 5
	
SELECT 
	tf.type_name
	, store_id
	, period_id
	, sales_volume
	, MIN(period_id) OVER() min_period_id
	, MAX(period_id) OVER() max_period_id
FROM public.sales s
JOIN top_five_stores tf USING(store_id)



SELECT * FROM public.store_chars sc 

/* Общее кол-во промо периодов */

SELECT 
	count(DISTINCT promo_period_id)
FROM sales_w_promo_periods


/* Медиана продолжительности промо периодов */

WITH period_durations AS(
SELECT
	promo_period_id
	, MAX(period_id) - MIN(period_id) + 1 period_duration
FROM sales_w_promo_periods
GROUP BY
	promo_period_id
)
SELECT
	PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY period_duration) median_period_duration
FROM period_durations

/* Объем продаж по каждому промопериоду */

SELECT
	promo_period_id
	, SUM(sales_volume) promo_period_sales_volume
FROM sales_w_promo_periods
GROUP BY
	promo_period_id
ORDER BY
	 SUM(sales_volume) DESC

/* Медиана количества промопериодов на один магазин */

WITH store_promo_period_cnt AS (
SELECT
	store_id
	, COUNT(DISTINCT promo_period_id) promo_period_cnt
FROM sales_w_promo_periods
GROUP BY
	store_id
)
SELECT
	PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY promo_period_cnt) median_promo_period_cnt
FROM store_promo_period_cnt
	 





