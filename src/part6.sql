CREATE OR REPLACE FUNCTION personal_offers_aimed_cross_selling(number_of_groups int,
                                                               max_churn numeric,
                                                               max_stability_index numeric,
                                                               max_sku_share numeric,
                                                               margin_share numeric)
    RETURNS TABLE
                (
                    customer_id bigint,
                    sku_name varchar,
                    offer_discount_depth int
                )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY
        WITH tab AS (SELECT t.transaction_store_id, t.group_id, t.sku_name, t.discount, t.sku_id
                     FROM (SELECT s.transaction_store_id,
                                  pg.group_id,
                                  s.sku_id,
                                  pg.sku_name,
                                  s.sku_retail_price - s.sku_purchase_price               AS marg,
                                  MAX(s.sku_retail_price - s.sku_purchase_price)
                                  OVER (PARTITION BY s.transaction_store_id, pg.group_id) AS max_marg,
                                  (s.sku_retail_price - s.sku_purchase_price) * max_SKU_share / 100 /
                                  s.sku_retail_price                                      AS discount

                           FROM stores s
                                    JOIN product_grid pg ON s.sku_id = pg.sku_id) t
                     WHERE marg = max_marg),

             tab1 AS (SELECT t.customer_id,
                             t.group_id
                      FROM (SELECT g.customer_id,
                                   g.group_id,
                                   ROW_NUMBER()
                                   OVER (PARTITION BY g.customer_id ORDER BY g.group_affinity_index DESC) group_array

                            FROM groups g

                            WHERE g.group_churn_rate < max_churn
                              AND g.group_stability_index < max_stability_index) t
                      WHERE group_array <= number_of_groups),

             tab3 AS (SELECT c.customer_id,
                             tab.transaction_store_id,
                             tab.group_id,
                             tab.sku_name,
                             tab.discount,
                             tab.sku_id

                      FROM tab
                               JOIN customers c ON c.customer_primary_store = tab.transaction_store_id),

             tab4 AS (SELECT t1.group_id, t2.sku_id, sku_count::numeric / group_count sku_share
                      FROM (SELECT pg.group_id, COUNT(DISTINCT c.transaction_id) group_count
                            FROM checks c
                                     JOIN product_grid pg ON c.sku_id = pg.sku_id
                            GROUP BY pg.group_id) t1
                               JOIN

                           (SELECT c.sku_id, pg.group_id, COUNT(DISTINCT c.transaction_id) sku_count
                            FROM checks c
                                     JOIN product_grid pg ON c.sku_id = pg.sku_id
                            GROUP BY c.sku_id, pg.group_id) t2
                           ON t1.group_id = t2.group_id

                      WHERE sku_count::numeric / group_count <= margin_share)

        SELECT t.customer_id          AS customer_id,
               tab3.sku_name          AS sku_name,
               t.Offer_Discount_Depth AS Offer_Discount_Depth
        FROM tab3
                 JOIN tab1 ON (tab3.customer_id, tab3.group_id) = (tab1.customer_id, tab1.group_id)
                 JOIN tab4 ON tab4.sku_id = tab3.sku_id
                 JOIN

             (SELECT g.customer_id, g.group_id, (g.group_minimum_discount * 100)::int / 5 * 5 + 5 Offer_Discount_Depth
              FROM groups g) t ON (t.customer_id, t.group_id) = (tab3.customer_id, tab3.group_id);
END;
$$;

-- SELECT *
--   FROM personal_offers_aimed_cross_selling(5, 3, 0.5, 100, 30);
