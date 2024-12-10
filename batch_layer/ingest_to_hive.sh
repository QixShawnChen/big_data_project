#!/bin/bash

option="jdbc:hive2://10.0.0.50:10001/default -n hadoop -d org.apache.hive.jdbc.HiveDriver"
#options="jdbc:hive2://localhost:10000/default -n hadoop -d org.apache.hive.jdbc.HiveDriver"

declare -a sql_files=("raw_data_table.hql" "formal_data_table.hql" "data_hbase.hql")

for sql_file in "${sql_files[@]}"; 
do
  echo "Executing $sql_file..."
  beeline -u ${option} -f $sql_file
  
  if [ $? -eq 0 ]; then
    echo "$sql_file executed successfully."
  else
    echo "Error executing $sql_file. Exiting."
    exit 1
  fi
done
