DROP TABLE IF EXISTS formal_data;

-- Create the formal_data table with the transformed structure
CREATE TABLE IF NOT EXISTS formal_data (
    country_id STRING,
    country_name STRING,
    year INT,
    NGDP_RPCH FLOAT,
    PCPIPCH FLOAT,
    PPPPC FLOAT,
    PPPGDP FLOAT,
    LP FLOAT,
    BCA FLOAT,
    LUR FLOAT,
    rev FLOAT,
    GGXCNL_NGDP FLOAT,
    NGS_GDP FLOAT,
    GGXCNL_GDP FLOAT,
    NI_GDP FLOAT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\"",
    "timestamp.formats" = "MM/dd/yyyy hh:mm:ss a"
)
STORED AS TEXTFILE;

-- Populate the formal_data table with transformed data
INSERT INTO TABLE formal_data
SELECT
    r.country_id AS country_id,
    r.country_name AS country_name,
    r.year AS year,
    MAX(CASE WHEN r.indicator = 'NGDP_RPCH' THEN r.value ELSE NULL END) AS NGDP_RPCH,
    MAX(CASE WHEN r.indicator = 'PCPIPCH' THEN r.value ELSE NULL END) AS PCPIPCH,
    MAX(CASE WHEN r.indicator = 'PPPPC' THEN r.value ELSE NULL END) AS PPPPC,
    MAX(CASE WHEN r.indicator = 'PPPGDP' THEN r.value ELSE NULL END) AS PPPGDP,
    MAX(CASE WHEN r.indicator = 'LP' THEN r.value ELSE NULL END) AS LP,
    MAX(CASE WHEN r.indicator = 'BCA' THEN r.value ELSE NULL END) AS BCA,
    MAX(CASE WHEN r.indicator = 'LUR' THEN r.value ELSE NULL END) AS LUR,
    MAX(CASE WHEN r.indicator = 'rev' THEN r.value ELSE NULL END) AS rev,
    MAX(CASE WHEN r.indicator = 'GGXCNL_NGDP' THEN r.value ELSE NULL END) AS GGXCNL_NGDP,
    MAX(CASE WHEN r.indicator = 'NGS_GDP' THEN r.value ELSE NULL END) AS NGS_GDP,
    MAX(CASE WHEN r.indicator = 'GGXCNL_GDP' THEN r.value ELSE NULL END) AS GGXCNL_GDP,
    MAX(CASE WHEN r.indicator = 'NI_GDP' THEN r.value ELSE NULL END) AS NI_GDP
FROM
    raw_data r
WHERE
    r.year < 2024
GROUP BY
    r.country_id, r.country_name, r.year;
