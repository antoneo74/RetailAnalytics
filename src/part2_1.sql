-- Таблица для поиска основного магазина клиента
CREATE OR REPLACE VIEW base_store AS
WITH tmp AS
         (SELECT p.customer_id,
                 ROW_NUMBER() OVER (PARTITION BY p.customer_id ORDER BY t.transaction_datetime DESC) AS raiting,
                 t.transaction_store_id,
                 COUNT(*) OVER (PARTITION BY p.customer_id, transaction_store_id)                    AS count

          FROM personal_information p
                   JOIN cards c ON p.customer_id = c.customer_id
                   JOIN transactions t ON c.customer_card_id = t.customer_card_id
          ORDER BY 1),

     tmp1 AS
         (SELECT DISTINCT customer_id, transaction_store_id AS last3
          FROM (SELECT *, COUNT(*) OVER (PARTITION BY customer_id, transaction_store_id) last3
                FROM tmp
                WHERE raiting IN (1, 2, 3)) t
          WHERE last3 = 3)

SELECT DISTINCT tmp.customer_id,
                tmp1.last3,
                FIRST_VALUE(tmp.transaction_store_id)
                OVER (PARTITION BY tmp.customer_id ORDER BY count DESC, tmp.raiting) max_value
FROM tmp
         LEFT JOIN tmp1 ON tmp1.customer_id = tmp.customer_id;


CREATE OR REPLACE VIEW Customers AS
-- Средний чек, количество покупок, дата последней покупки клиента, интервал в днях между первой и последней транзакцией
WITH t AS
         (SELECT p.customer_id               AS Customer_ID,
                 AVG(t.transaction_summ)     AS Customer_Average_Check,
                 COUNT(*)                    AS count,
                 MAX(t.transaction_datetime) AS last_buy,
                 EXTRACT(EPOCH FROM MAX(t.transaction_datetime) - MIN(t.transaction_datetime))::numeric /
                 86400                       AS days_interval
          FROM personal_information p
                   JOIN cards c ON p.customer_id = c.customer_id
                   JOIN transactions t ON t.customer_card_id = c.customer_card_id
          GROUP BY p.customer_id),

     -- Колонки Customer_Average_Check_Segment и Customer_Frequency
     t1 AS
         (SELECT *,
                 CASE
                     WHEN (RANK() OVER (ORDER BY Customer_Average_Check DESC) <= (SELECT COUNT(*) FROM t) * 0.1)
                         THEN 'High'
                     WHEN (RANK() OVER (ORDER BY Customer_Average_Check DESC) <= (SELECT COUNT(*) FROM t) * 0.35)
                         THEN 'Medium'
                     ELSE 'Low' END        AS Customer_Average_Check_Segment,
                 t.days_interval / t.count AS Customer_Frequency
          FROM t),

     -- Колонки Customer_Frequency_Segment, Customer_Inactive_Period, Customer_Churn_Rate
     t2 AS (SELECT *,
                   CASE
                       WHEN (RANK() OVER (ORDER BY Customer_Frequency) <= (SELECT COUNT(*) FROM t) * 0.1)
                           THEN 'Often'
                       WHEN (RANK() OVER (ORDER BY Customer_Frequency) <= (SELECT COUNT(*) FROM t) * 0.35)
                           THEN 'Occasionally'
                       ELSE 'Rarely' END AS Customer_Frequency_Segment,
                   (SELECT EXTRACT(EPOCH FROM (SELECT MAX(analysis_formation) FROM date_of_analysis_formation) -
                                              t1.last_buy)::numeric /
                           86400)        AS Customer_Inactive_Period,
                   (SELECT EXTRACT(EPOCH FROM (SELECT MAX(analysis_formation) FROM date_of_analysis_formation) -
                                              t1.last_buy)::numeric / 86400) /
                   t1.Customer_Frequency AS Customer_Churn_Rate
            FROM t1),

     -- Колонка Customer_Churn_Segment
     t3 AS (SELECT *,
                   CASE
                       WHEN (t2.Customer_Churn_Rate < 2) THEN 'Low'
                       WHEN (t2.Customer_Churn_Rate < 5) THEN 'Medium'
                       ELSE 'High' END AS Customer_Churn_Segment
            FROM t2)

SELECT t3.Customer_ID,
       t3.Customer_Average_Check,
       t3.Customer_Average_Check_Segment,
       t3.Customer_Frequency,
       t3.Customer_Frequency_Segment,
       t3.Customer_Inactive_Period,
       t3.Customer_Churn_Rate,
       t3.Customer_Churn_Segment,
       cs.segment AS Customer_Segment,
       CASE
           WHEN bs.last3 IS NOT NULL THEN bs.last3
           ELSE bs.max_value
           END    AS Customer_Primary_Store

FROM t3
         JOIN base_store bs
              ON bs.customer_id = t3.customer_id
         JOIN customer_segment cs
              ON (t3.Customer_Average_Check_Segment, t3.Customer_Frequency_Segment, t3.Customer_Churn_Segment) =
                 (cs.Customer_Average_Check, cs.Customer_Frequency, cs.customer_churn_rate)
;

-- SELECT * FROM Customers
-- WHERE customer_id = 3;
-- 
-- SELECT * FROM Customers
-- WHERE customer_segment = 10;
