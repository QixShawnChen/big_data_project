-- Drop the HBase-backed Hive table if it exists
DROP TABLE IF EXISTS formal_data_speed_hbase;

-- Create the HBase-backed Hive table
CREATE EXTERNAL TABLE formal_data_speed_hbase (
    composite_key STRING,
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
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    'hbase.columns.mapping' = ':key,info:country_id,info:country_name,info:year,info:NGDP_RPCH,info:PCPIPCH,info:PPPPC,info:PPPGDP,info:LP,info:BCA,info:LUR,info:rev,info:GGXCNL_NGDP,info:NGS_GDP,info:GGXCNL_GDP,info:NI_GDP'
)
TBLPROPERTIES (
    'hbase.table.name' = 'qixshawnchen_final_project_speed_hbase'
);


