#!/bin/bash

# Script to manage HBase table: disable, drop (if exists), and recreate

TABLE_NAME="qixshawnchen_final_project_speed_hbase"
COLUMN_FAMILY="info"

# Execute HBase commands in a single shell script
hbase shell <<EOF
if (exists '$TABLE_NAME')
  disable '$TABLE_NAME'
  drop '$TABLE_NAME'
end
create '$TABLE_NAME', '$COLUMN_FAMILY'
EOF

# Confirm the process   #create 'qixshawnchen_final_project_speed_hbase', 'info'
echo "Table $TABLE_NAME has been recreated successfully."