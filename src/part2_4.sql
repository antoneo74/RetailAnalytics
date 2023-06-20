CREATE OR REPLACE FUNCTION get_analysis_table(days_count integer, tr_count integer)
    RETURNS TABLE
                (
                    customer_id bigint,
                    group_id bigint,
                    group_margin numeric
                )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY
        SELECT t.customer_id, t.group_id, SUM(t.group_summ_paid - t.group_cost) AS Group_Margin
        FROM (SELECT *
              FROM (SELECT *, DENSE_RANK() OVER (ORDER BY transaction_datetime DESC) rank
                    FROM purchase_history) a
              WHERE rank <= (CASE
                                 WHEN (tr_count IS NULL) THEN
                                         (SELECT COUNT(DISTINCT transaction_id) FROM transactions)
                                 ELSE tr_count END)) t
        WHERE transaction_datetime >= (CASE
                                           WHEN (days_count IS NULL) THEN
                                                   (SELECT MIN(transaction_datetime) FROM purchase_history)
                                           ELSE (SELECT MAX(analysis_formation) FROM date_of_analysis_formation)::date -
                                                days_count END)
        GROUP BY 1, 2;
END;
$$;

------------------------------------------------------------------------------------------------------------------------
CREATE MATERIALIZED VIEW group_margin AS
SELECT *
FROM get_analysis_table(NULL, NULL);
------------------------------------------------------------------------------------------------------------------------

CREATE MATERIALIZED VIEW aff AS
         (-- Колонки Customer_id, Group_id, Group_Affinity_Index
             SELECT customer_id,
                    group_id,
                    group_purchase / COUNT(DISTINCT transaction_id)::numeric AS Group_Affinity_Index
             FROM (SELECT p.customer_id, p.group_id, group_purchase, transaction_id
                   FROM periods p
                            JOIN purchase_history ph ON ph.customer_id = p.customer_id
                       AND ph.transaction_datetime BETWEEN p.first_group_purchase_date AND p.last_group_purchase_date) t
             GROUP BY 1, 2, group_purchase);

CREATE MATERIALIZED VIEW gcr AS
       (-- Колонка Group_Churn_Rate
           SELECT customer_id, group_id,
                  EXTRACT(EPOCH FROM ((SELECT MAX(analysis_formation)
                                         FROM date_of_analysis_formation) -
                                      p.last_group_purchase_date) / 86400) /
                  p.group_frequency AS group_churn_rate
           FROM periods p);

CREATE MATERIALIZED VIEW gsi AS
     (-- Колонка Group_Stability_Index
         SELECT customer_id, group_id, AVG(c) AS Group_Stability_Index
         FROM (SELECT p.customer_id, p.group_id,
                      ABS(EXTRACT(EPOCH FROM (transaction_datetime -
                                              LAG(transaction_datetime, 1)
                                              OVER (PARTITION BY ph.customer_id, ph.group_id ORDER BY transaction_datetime)) /
                                             86400::numeric) -
                          p.group_frequency) / p.group_frequency AS c
                 FROM purchase_history ph
                          JOIN periods p ON (ph.customer_id, ph.group_id) = (p.customer_id, p.group_id)) t
         GROUP BY 1, 2);

CREATE MATERIALIZED VIEW gds AS
     (-- Колонка Group_Discount_Share
         SELECT p.customer_id, p.group_id, disc_cnt::numeric / p.group_purchase AS Group_Discount_Share
         FROM (SELECT t.customer_id, pg.group_id, COUNT(DISTINCT tr.transaction_id) AS disc_cnt
           FROM (SELECT pi.customer_id, c.customer_card_id
                   FROM personal_information pi
                            JOIN cards c
                            ON c.customer_id = pi.customer_id) t
                    JOIN transactions tr ON t.customer_card_id = tr.customer_card_id
                    JOIN checks c2 ON tr.transaction_id = c2.transaction_id
                    JOIN product_grid pg ON c2.sku_id = pg.sku_id
               WHERE sku_discount > 0
               GROUP BY 1, 2) t
                  JOIN periods p ON (t.customer_id, t.group_id) = (p.customer_id, p.group_id));


CREATE materialized VIEW gad AS
      (-- Колонка Group_Average_Discount
          SELECT customer_id, group_id, SUM(group_summ_paid) / SUM(group_summ) AS Group_Average_Discount
          FROM purchase_history
          GROUP BY 1, 2);

CREATE MATERIALIZED VIEW groups
AS
SELECT aff.customer_id,
       aff.group_id,
       aff.Group_Affinity_Index,
       gcr.Group_Churn_Rate,
       COALESCE(gsi.Group_Stability_Index, 0) AS Group_Stability_Index,
       gm.group_margin,
       COALESCE(gds.Group_Discount_Share, 0)  AS Group_Discount_Share,
       p.group_min_discount                   AS Group_Minimum_Discount,
       gad.Group_Average_Discount
  FROM aff
           JOIN gcr
           ON (aff.customer_id, aff.group_id) = (gcr.customer_id, gcr.group_id)
           JOIN gsi
           ON (aff.customer_id, aff.group_id) = (gsi.customer_id, gsi.group_id)
      -- Колонка Group_Margin
           JOIN group_margin gm
           ON (aff.customer_id, aff.group_id) = (gm.customer_id, gm.group_id)
           LEFT JOIN gds
           ON (aff.customer_id, aff.group_id) = (gds.customer_id, gds.group_id)
      -- Колонка Group_Minimum_Discount
           JOIN periods p
           ON (aff.customer_id, aff.group_id) = (p.customer_id, p.group_id)
           JOIN gad
           ON (aff.customer_id, aff.group_id) = (gad.customer_id, gad.group_id);

------------------------------------------------------------------------------------------------------------------------

-- SELECT * FROM groups;
