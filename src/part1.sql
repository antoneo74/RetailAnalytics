-- CREATE database RetailAnalytics

CREATE TABLE IF NOT EXISTS personal_information
(
    Customer_ID            bigint PRIMARY KEY,
    Customer_Name          varchar(50)  NOT NULL CHECK (Customer_Name ~ '^([А-ЯЁ]{1}[а-яё \-]*|[A-Z]{1}[a-z \-]*)$'),
    Customer_Surname       varchar(50)  NOT NULL CHECK (Customer_Name ~ '^([А-ЯЁ]{1}[а-яё \-]*|[A-Z]{1}[a-z \-]*)$'),
    Customer_Primary_Email varchar(255) NOT NULL CHECK (Customer_Primary_Email ~
                                                        '^((([0-9A-Za-z]{1}[-0-9A-z\.]{1,}[0-9A-Za-z]{1}))@([-A-Za-z]{1,}\.){1,2}[-A-Za-z]{2,})$'),
    Customer_Primary_Phone varchar(12)  NOT NULL CHECK (Customer_Primary_Phone ~ '^\+7[0-9]{10}$')
);

CREATE TABLE IF NOT EXISTS cards
(
    Customer_Card_ID bigint PRIMARY KEY,
    Customer_ID      bigint,
    FOREIGN KEY (Customer_ID) REFERENCES personal_information (Customer_ID)
);


CREATE TABLE IF NOT EXISTS SKU_group
(
    Group_ID   bigint PRIMARY KEY,
    Group_Name varchar(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS date_of_analysis_formation
(
    Analysis_Formation timestamp WITHOUT TIME ZONE
);

CREATE TABLE IF NOT EXISTS product_grid
(
    SKU_ID   bigint PRIMARY KEY,
    SKU_Name varchar(255) NOT NULL,
    Group_ID bigint       NOT NULL,
    FOREIGN KEY (Group_ID) REFERENCES SKU_group (Group_ID)
);

CREATE TABLE IF NOT EXISTS stores
(
    Transaction_Store_ID bigint NOT NULL,
    SKU_ID               bigint NOT NULL,
    FOREIGN KEY (SKU_ID) REFERENCES product_grid (SKU_ID),
    SKU_Purchase_Price   numeric,
    SKU_Retail_Price     numeric
);

CREATE TABLE IF NOT EXISTS transactions
(
    Transaction_ID       bigint PRIMARY KEY,
    Customer_Card_ID     bigint,
    FOREIGN KEY (Customer_Card_ID) REFERENCES cards (Customer_Card_ID),
    Transaction_Summ     numeric,
    Transaction_DateTime timestamp,
    Transaction_Store_ID bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS checks
(
    Transaction_ID bigint,
    FOREIGN KEY (Transaction_ID) REFERENCES transactions (Transaction_ID),
    SKU_ID         bigint,
    FOREIGN KEY (SKU_ID) REFERENCES product_grid (SKU_ID),
    SKU_Amount     numeric,
    SKU_Summ       numeric,
    SKU_Summ_Paid  numeric,
    SKU_Discount   numeric
);

CREATE TABLE IF NOT EXISTS Customer_Segment
(
    Segment                int,
    Customer_Average_Check varchar(20) NOT NULL,
    Customer_Frequency     varchar(20) NOT NULL,
    Customer_Churn_Rate    varchar(20) NOT NULL
);

CREATE OR REPLACE PROCEDURE from_csv_tsv(path text, separator char)
    LANGUAGE plpgsql
AS
$$
DECLARE
    extension varchar(4);
BEGIN
    IF (separator = ',') THEN
        extension = '.csv';
    ELSEIF separator = '\t'
    THEN
        extension = '.tsv';
    END IF;

    EXECUTE
        ('COPY personal_information (Customer_ID,Customer_Name,Customer_Surname,Customer_Primary_Email,Customer_Primary_Phone) FROM '''
             || path
             || '/Personal_Data_Mini' || extension || ''' WITH (FORMAT CSV, DELIMITER E'''
             || separator
            || ''')');

    EXECUTE
        ('COPY cards (Customer_Card_ID,Customer_ID) FROM '''
             || path
             || '/Cards_Mini' || extension || ''' WITH (FORMAT CSV, DELIMITER E'''
             || separator
            || ''')');

    EXECUTE
        ('COPY SKU_group (Group_ID, Group_Name) FROM '''
             || path
             || '/Groups_SKU_Mini' || extension || ''' WITH (FORMAT CSV, DELIMITER E'''
             || separator
            || ''')');

    EXECUTE
        ('COPY date_of_analysis_formation (Analysis_Formation) FROM '''
             || path
             || '/Date_Of_Analysis_Formation' || extension || ''' WITH (FORMAT CSV, DELIMITER E'''
             || separator
            || ''')');

    EXECUTE
        ('COPY product_grid (SKU_ID, SKU_Name, Group_ID) FROM '''
             || path
             || '/SKU_Mini' || extension || ''' WITH (FORMAT CSV, DELIMITER E'''
             || separator
            || ''')');

    EXECUTE
        ('COPY stores (Transaction_Store_ID, SKU_ID, SKU_Purchase_Price, SKU_Retail_Price) FROM '''
             || path
             || '/Stores_Mini' || extension || ''' WITH (FORMAT CSV, DELIMITER E'''
             || separator
            || ''')');

    EXECUTE
        ('COPY transactions (Transaction_ID, Customer_Card_ID, Transaction_Summ, Transaction_DateTime, Transaction_Store_ID) FROM '''
             || path
             || '/Transactions_Mini' || extension || ''' WITH (FORMAT CSV, DELIMITER E'''
             || separator
            || ''')');

    EXECUTE
        ('COPY checks (Transaction_ID, SKU_ID, SKU_Amount, SKU_Summ, SKU_Summ_Paid, SKU_Discount) FROM '''
             || path
             || '/Checks_Mini' || extension || ''' WITH (FORMAT CSV, DELIMITER E'''
             || separator
            || ''')');
    EXECUTE
        ('COPY Customer_Segment (Segment, Customer_Average_Check, Customer_Frequency, Customer_Churn_Rate) FROM '''
             || path
             || '/Customer_Segment' || extension || ''' WITH (FORMAT CSV, DELIMITER E'''
             || separator
            || ''')');
END
$$
;

-- Необходимо указать абсолютный путь до папки 
CALL from_csv_tsv('SQL3_RetailAnalitycs_v1.0-1/src/datasets', '\t');


CREATE OR REPLACE PROCEDURE to_csv_tsv(path text, separator char)
    LANGUAGE plpgsql
AS
$$
DECLARE
    extension varchar(4);
BEGIN
    IF (separator = ',') THEN
        extension = '.csv';
    ELSEIF separator = '\t'
    THEN
        extension = '.tsv';
    END IF;

    EXECUTE
        ('COPY (select * from personal_information) TO '''
             || path
             || '/Personal_Data' || extension || ''' WITH (FORMAT CSV, DELIMITER E'''
             || separator
            || ''')');

    EXECUTE
        ('COPY (select * from cards) TO '''
             || path
             || '/Cards' || extension || ''' WITH (FORMAT CSV, DELIMITER E'''
             || separator
            || ''')');

    EXECUTE
        ('COPY (select * from SKU_group) TO '''
             || path
             || '/Groups_SKU' || extension || ''' WITH (FORMAT CSV, DELIMITER E'''
             || separator
            || ''')');

    EXECUTE
        ('COPY (select * from date_of_analysis_formation) TO '''
             || path
             || '/Date_Of_Analysis_Formation' || extension || ''' WITH (FORMAT CSV, DELIMITER E'''
             || separator
            || ''')');

    EXECUTE
        ('COPY (select * from product_grid) TO '''
             || path
             || '/SKU' || extension || ''' WITH (FORMAT CSV, DELIMITER E'''
             || separator
            || ''')');

    EXECUTE
        ('COPY (select * from stores) TO '''
             || path
             || '/Stores' || extension || ''' WITH (FORMAT CSV, DELIMITER E'''
             || separator
            || ''')');

    EXECUTE
        ('COPY (select * from transactions) TO '''
             || path
             || '/Transactions' || extension || ''' WITH (FORMAT CSV, DELIMITER E'''
             || separator
            || ''')');

    EXECUTE
        ('COPY (select * from checks) TO '''
             || path
             || '/Checks' || extension || ''' WITH (FORMAT CSV, DELIMITER E'''
             || separator
            || ''')');
END
$$
;

-- Необходимо указать абсолютный путь до папки
CALL to_csv_tsv('SQL3_RetailAnalitycs_v1.0-1/src/datasets', ',')
;
