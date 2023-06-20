DROP OWNED BY administrator;
DROP OWNED BY visitor;
DROP ROLE IF EXISTS administrator;
DROP ROLE IF EXISTS visitor;

DO
$$
    DECLARE
        database text := (SELECT CURRENT_DATABASE());
        schema   text := (SELECT CURRENT_SCHEMA);

    BEGIN
        CREATE ROLE administrator WITH CREATEDB CREATEROLE LOGIN PASSWORD 'super_password';
        EXECUTE 'GRANT ALL ON DATABASE ' || database || ' TO administrator';
        EXECUTE 'GRANT ' || database || ' TO administrator';

        CREATE ROLE visitor LOGIN PASSWORD 'password';
        EXECUTE 'GRANT USAGE ON SCHEMA ' || schema || ' TO visitor';
        EXECUTE 'GRANT SELECT ON ALL TABLES IN SCHEMA ' || schema ||
                ' TO visitor';
    END
$$;

-- check
SELECT *
FROM pg_roles
WHERE rolname IN ('administrator', 'visitor');

--------------------------------------------------------------------------------
SET ROLE = 'administrator';
SHOW ROLE;

SET ROLE = 'visitor';
SHOW ROLE;

-- действие возможно для обеих ролей
SELECT *
FROM checks
WHERE sku_summ > 1000;

-- действие возможно только для админа
INSERT INTO product_grid (sku_id, sku_name, group_id)
VALUES ((SELECT MAX(sku_id) + 1 FROM product_grid),
        'Faber-Castell Ручка капиллярная', '8');

DELETE
  FROM product_grid
 WHERE sku_name = 'Faber-Castell Ручка капиллярная';

-- SET ROLE = 'postgres';
