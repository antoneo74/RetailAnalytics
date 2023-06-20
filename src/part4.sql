CREATE OR REPLACE FUNCTION get_period_table(date_begin date, date_end date,
                                            cef_increase_check numeric)
    RETURNS TABLE
                (
                    customer_id bigint,
                    required_check_measure numeric
                )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY SELECT p.customer_id AS customer_id,
                        SUM(transaction_summ) / COUNT(*) *
                        cef_increase_check AS required_check_measure
                   FROM personal_information p
                            JOIN cards c ON p.customer_id = c.customer_id
                            JOIN transactions t ON t.customer_card_id = c.customer_card_id
                  WHERE transaction_datetime::date >= date_begin
                    AND transaction_datetime::date <= date_end
                  GROUP BY p.customer_id;
END;
$$;

CREATE OR REPLACE FUNCTION get_transaction_table(transactions_count int,
                                                 cef_increase_check numeric)
    RETURNS TABLE
                (
                    customer_id bigint,
                    required_check_measure numeric
                )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY
        SELECT t1.customer_id                                                     AS customer_id,
               SUM(transaction_summ) / COUNT(transaction_id) * cef_increase_check AS Required_Check_Measure
        FROM (SELECT p.customer_id,
                     transaction_id,
                     transaction_summ,
                     ROW_NUMBER()
                     OVER (PARTITION BY p.customer_id ORDER BY transaction_datetime DESC) AS count
              FROM personal_information p
                       JOIN cards c ON p.customer_id = c.customer_id
                       JOIN transactions t ON t.customer_card_id = c.customer_card_id) t1
        WHERE count <= transactions_count
        GROUP BY t1.customer_id;
END;
$$;

-- SELECT *
-- FROM get_transaction_table(100, 1.15);

-- SELECT *
-- FROM get_period_table('2016-01-01', '2023-01-01', 1.15);


-- Расчитываем процент маржинальности по каждой группе
-- (делим среднюю фактически полученную прибыль (оплаченная сумма - себестоимость)
-- на среднюю себестоимость
-- если < 0 значит товар продан в убыток

CREATE OR REPLACE FUNCTION get_discount(max_churn numeric,
                                        max_discount_share numeric,
                                        max_marge_share numeric)
    RETURNS TABLE
                (
                    customer_id bigint,
                    group_id bigint,
                    discount int
                )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY
        WITH t1 AS (SELECT ph.customer_id,
                           ph.group_id,
                           CASE
                               WHEN ((SUM(ph.group_summ_paid - ph.group_cost) / COUNT(*)) < 0) THEN 0
                               ELSE (SUM(ph.group_summ_paid - ph.group_cost) / SUM(ph.group_cost) * max_marge_share
                                   ) END AS marg
                    FROM purchase_history ph
                    GROUP BY ph.customer_id, ph.group_id),

             t2 AS (SELECT g.customer_id,
                           g.group_id,
                           g.group_affinity_index,
                           (g.group_minimum_discount * 100)::int / 5 * 5 + 5 AS                                Discount,
                           ROW_NUMBER() OVER (PARTITION BY g.customer_id ORDER BY g.group_affinity_index DESC) raiting
                    FROM groups g
                    WHERE g.group_churn_rate <= max_churn
                      AND g.group_discount_share < (max_discount_share / 100::numeric)),
             t3 AS (SELECT t1.customer_id,
                           t1.group_id,
                           CASE
                               WHEN (t2.raiting = MIN(t2.raiting) OVER (PARTITION BY t1.customer_id )) THEN t2.Discount
                               END AS Discount
                    FROM t1
                             JOIN t2
                                  ON (t1.customer_id, t1.group_id) = (t2.customer_id, t2.group_id)
                    WHERE t2.Discount < marg)
        SELECT *
          FROM t3
         WHERE t3.discount IS NOT NULL;
END;
$$;

-- SELECT * FROM get_discount(3,70,30);

CREATE OR REPLACE FUNCTION personal_offers(method int, date_begin date,
                                           date_end date,
                                           transactions_count int,
                                           cef_increase_check numeric,
                                           max_churn numeric,
                                           max_discount_share numeric,
                                           max_marge_share numeric)
    RETURNS TABLE
                (
                    customer_id bigint,
                    required_check_measure numeric,
                    group_name varchar,
                    offer_discount_depth int
                )
    LANGUAGE plpgsql
AS
$$
BEGIN
    DROP TABLE IF EXISTS tmp;
    CREATE TEMP TABLE tmp
        (
            customer bigint,
            required_check_measure numeric
        );
    IF method = 1 THEN
        INSERT INTO tmp SELECT * FROM get_period_table(date_begin, date_end, cef_increase_check);
    ELSIF method = 2 THEN
        INSERT INTO tmp SELECT * FROM get_transaction_table(transactions_count, cef_increase_check);
    END IF;

    RETURN QUERY
        SELECT t1.customer_id             AS customer_id,
               tmp.Required_Check_Measure AS Required_Check_Measure,
               sk.group_name              AS group_name,
               t1.Discount                AS Offer_Discount_Depth

        FROM (SELECT *
              FROM get_discount(max_churn, max_discount_share,
                                max_marge_share)) t1
                 JOIN sku_group sk
                      ON sk.group_id = t1.group_id

                 JOIN tmp ON tmp.customer = t1.customer_id;
END;
$$;

-- SELECT *
-- FROM personal_offers(2, '2016-01-01', '2023-01-01', 100, 1.15, 3, 70, 30);
