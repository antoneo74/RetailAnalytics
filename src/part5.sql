CREATE OR REPLACE FUNCTION personal_offers_aimed_increasing_frequency(date_begin timestamp,
                                                                      date_end timestamp,
                                                                      adding_transactions_count int,
                                                                      max_churn numeric,
                                                                      max_discount_share numeric,
                                                                      max_marge_share numeric)
    RETURNS TABLE
                (
                    customer_id bigint,
                    start_date timestamp,
                    end_date timestamp,
                    required_transactions_count int,
                    group_name varchar,
                    offer_discount_depth int
                )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY
        SELECT c.customer_id             AS customer_id,
               date_begin                AS Start_Date,
               date_end                  AS End_Date,
               ROUND(EXTRACT(EPOCH FROM (date_end - date_begin) / 86400::numeric) / c.customer_frequency)::int +
               adding_transactions_count AS Required_Transactions_Count,
               sk.group_name             AS group_name,
               t1.discount               AS Offer_Discount_Depth
        FROM customers c
                 JOIN (SELECT *
                         FROM get_discount(max_churn, max_discount_share,
                                           max_marge_share)) t1
                 ON c.customer_id = t1.customer_id
                 JOIN sku_group sk
                 ON sk.group_id = t1.group_id;
END;
$$;

-- SELECT *
--   FROM personal_offers_aimed_increasing_frequency('2022-08-19 00:00:00.0000000',
--                                                   '2022-08-18 00:00:00.0000000',
--                                                   1, 3, 70, 30)