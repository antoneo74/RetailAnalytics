CREATE OR REPLACE VIEW periods
AS
  WITH min_discount AS (SELECT c.transaction_id, pg.group_id,
                               MIN(c.sku_discount / c.sku_summ) AS discount
                          FROM checks c
                                   JOIN product_grid pg ON c.sku_id = pg.sku_id
                         WHERE sku_discount <> 0
                         GROUP BY c.transaction_id, pg.group_id)

SELECT p.customer_id AS customer_id,
       p.group_id AS group_id,
       MIN(p.transaction_datetime) AS first_group_purchase_date,
       MAX(p.transaction_datetime) AS last_group_purchase_date,
       COUNT(*) AS group_purchase,
       (EXTRACT(EPOCH FROM MAX(p.transaction_datetime) -
                           MIN(p.transaction_datetime))::numeric / 86400 + 1) /
       COUNT(*) AS group_frequency,
       COALESCE(MIN(discount), 0) AS group_min_discount
  FROM purchase_history p
           LEFT JOIN min_discount t
           ON (p.transaction_id, p.group_id) = (t.transaction_id, t.group_id)
 GROUP BY p.customer_id, p.group_id;

-- SELECT *
--   FROM periods
--  ORDER BY 1, 2;
