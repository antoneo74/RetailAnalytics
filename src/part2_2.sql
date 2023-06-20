CREATE OR REPLACE VIEW purchase_history
AS
SELECT customer_id AS customer_id, 
       transaction_id AS transaction_id,
       transaction_datetime AS transaction_datetime, 
       group_id AS group_id,
       SUM(sku_amount * sku_purchase_price) AS group_cost,
       SUM(sku_summ) AS group_summ, 
       SUM(sku_summ_paid) AS group_summ_paid
  FROM (SELECT c.customer_id, t.transaction_id, t.transaction_datetime,
               p.group_id, ch.sku_amount, s.sku_purchase_price, ch.sku_summ,
               ch.sku_summ_paid
          FROM personal_information pi
                   JOIN cards c ON pi.customer_id = c.customer_id
                   JOIN transactions t ON c.customer_card_id = t.customer_card_id
                   JOIN checks ch ON t.transaction_id = ch.transaction_id
                   JOIN product_grid p ON ch.sku_id = p.sku_id
                   JOIN stores s ON (t.transaction_store_id, ch.sku_id) =
                      (s.transaction_store_id, s.sku_id)) t
 GROUP BY customer_id, transaction_id, transaction_datetime, group_id;

-- SELECT * FROM purchase_history;
