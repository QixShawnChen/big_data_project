DROP TABLE IF EXISTS raw_data;

-- Create the raw data table
CREATE TABLE IF NOT EXISTS raw_data (
    indicator STRING,
    country_id STRING,
    country_name STRING,
    year INT,
    value FLOAT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\"",
    "timestamp.formats" = "MM/dd/yyyy hh:mm:ss a"
)
STORED AS TEXTFILE;

-- Load data into the raw_data table
LOAD DATA INPATH '/tmp/qixshawnchen/final_data.csv' INTO TABLE raw_data;




