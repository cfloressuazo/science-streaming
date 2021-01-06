-- streams
CREATE STREAM medicare_stream_01 WITH (KAFKA_TOPIC='medicare', value_format='AVRO');
CREATE STREAM medicare_stream_02 WITH (KAFKA_TOPIC='medicare', value_format='AVRO', partitions=1);
CREATE STREAM medicare_stream_ny AS SELECT * FROM medicare_stream_01 WHERE NPPES_PROVIDER_STATE = 'NY';
CREATE STREAM medicare_stream_99213 AS SELECT * FROM medicare_stream_01 WHERE HCPCS_CODE = '99213';
CREATE STREAM MEDICARE_STREAM_PROCEDURE_PROVIDER AS SELECT HCPCS_CODE, HCPCS_DESCRIPTION, NPI FROM medicare_stream_01 PARTITION BY HCPCS_CODE;

-- tables
CREATE TABLE PROCEDURE_BY_PROVIDERS AS SELECT HCPCS_CODE, HCPCS_DESCRIPTION, COUNT(1) AS COUNT_PROVIDERS FROM MEDICARE_STREAM_PROCEDURE_PROVIDER GROUP BY HCPCS_CODE, HCPCS_DESCRIPTION;

-- top ten Healthcare Common Procedure Coding System (HCPCS) codes in volume
select
    HCPCS_DESCRIPTION,
    count(*) as HCPCS_COUNT,
    avg(cast(AVERAGE_MEDICARE_PAYMENT_AMT as DOUBLE)) as AVERAGE_MEDICARE_PAYMENT_AMT,
    sum(cast(AVERAGE_SUBMITTED_CHRG_AMT as decimal(9,2))) as TOTAL_PAYMENT_AMT
from
    medicare_stream_01
group by
    HCPCS_DESCRIPTION
EMIT CHANGES;

-- aggregate State, City in volume
create TABLE STATE_CITY_AGG AS
    select
        NPPES_PROVIDER_STATE,
        NPPES_PROVIDER_CITY,
        count(*) as COUNT_STATE_CITY,
        avg(cast(AVERAGE_MEDICARE_PAYMENT_AMT as DOUBLE)) as AVERAGE_MEDICARE_PAYMENT_AMT,
        sum(cast(AVERAGE_MEDICARE_ALLOWED_AMT as decimal(9,2))) as TOTAL_PAYMENT_AMT
    from
        medicare_stream_01
    group by
        NPPES_PROVIDER_STATE,
        NPPES_PROVIDER_CITY
    EMIT CHANGES
;

-- aggregate State, City in volume for 1 minute
create TABLE STATE_CITY_AGG_PER_MIN AS
    select
        NPPES_PROVIDER_STATE,
        NPPES_PROVIDER_CITY,
        count(*) as COUNT_STATE_CITY,
        avg(cast(AVERAGE_MEDICARE_PAYMENT_AMT as DOUBLE)) as AVERAGE_MEDICARE_PAYMENT_AMT,
        sum(cast(AVERAGE_MEDICARE_ALLOWED_AMT as decimal(9,2))) as TOTAL_PAYMENT_AMT
    from
        medicare_stream_01
    WINDOW TUMBLING (SIZE 1 MINUTE)
    GROUP BY
        NPPES_PROVIDER_STATE,
        NPPES_PROVIDER_CITY
    EMIT CHANGES
;
-- How many times a Established patient office or other outpatient visit is done in NY?
SELECT
    NPPES_PROVIDER_STATE,
    COUNT(*) AS COUNT_PROCEDURE_PER_REGION
FROM
    medicare_stream_01
WHERE
    HCPCS_DESCRIPTION = 'Established patient office or other outpatient visit, typically 15 minutes'
    AND NPPES_PROVIDER_STATE = 'NY'
GROUP BY
    NPPES_PROVIDER_STATE
EMIT CHANGES;

-- how many regions is a procedure (99213) done in?
CREATE TABLE PROCEDURE_STATISTICS_99213_01 AS
SELECT
    HCPCS_CODE,
    NPPES_PROVIDER_STATE,
    NPPES_PROVIDER_CITY,
    COUNT(*) AS COUNT_PROCEDURE,
    ((MAX(AVERAGE_MEDICARE_PAYMENT_AMT) * 100) / MAX(AVERAGE_SUBMITTED_CHRG_AMT)) AS PERCENTAGE_MEDICARE_PAYMENT_VS_SUBMITTED,
    MAX(AVERAGE_SUBMITTED_CHRG_AMT) AS AVERAGE_SUBMITTED_CHRG_AMT,
    MAX(AVERAGE_MEDICARE_PAYMENT_AMT) AS AVERAGE_MEDICARE_PAYMENT_AMT
FROM
    medicare_stream_99213
GROUP BY
    HCPCS_CODE,
    NPPES_PROVIDER_STATE,
    NPPES_PROVIDER_CITY
;
-- How many times a procedure is done in a specific region?
 SELECT * FROM PROCEDURE_STATISTICS_99213_01 WHERE KSQL_COL_0='99213|+|NJ|+|MEDFORD' EMIT CHANGES;

-- Which procedures have the fewest providers?


-- How much does the cost of a procedure vary?