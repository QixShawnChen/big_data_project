Big Data Final Project:
![Alt text](./readMe1.png)

Overview:
For this project, I am using the worldwide economic data to implement the BIG DATA Web App. The web app will provide the Real GDP growth, Inflation rate, GDP per capita, PPP GDP, Population, NGDPD, Current account balance, Unemployment rate, Government revenue, and General government net lending of 36 countries. 
Since Economic data is not that variable, it will be updated each quarter in a year generally. Therefore, I design the the Big Data System to display the economic data of previous year through batch layer and the economic data of the current year through speed layer. 

"The port I am using is 3088" (http://localhost:3088/)

"The Economic data before 2024 are accessed through batch layer. The Economic data of 2024 are accessed through speed layer."

Data Source:
The data are collected from International Monetary Fund (IMF). They provide API. (https://www.imf.org/external/datamapper/api/v1/INDICATOR/{indicator}/{country_id})

Data Collection:
The script data_collector.py is used to fetch the necessary data by calling the API multiple times. The script processes and formats the data into a simplified CSV file. This dataset is then transferred to the Hadoop File System (HDFS) using the following command embedded in the Python script:
 “hdfs dfs -put -f /tmp/qixshawnchen/final_project/final_data.csv” 

Batch Layer: 
In this layer, corresponding tables are created in HIVE using the scripts raw_data_table.hql and formal_data_table.hql. The tables in HIVE are named raw_data and formal_data. After the successful creation of these tables, the batch layer table is created in HBase. The table for the batch layer in HBase is named qixshawnchen_final_project_hbase.
The commands to perform this setup are included in the script:
/home/sshuser/qixshawnchen/final_project/batch_layer/ingest_to_hive.sh.

![Alt text](./readMe2.png "Batch Layer: Years before 2024.")

Speed Layer:
The speed layer handles real-time data for the year 2024. It fetches the latest economic data from the IMF API:
(https://www.imf.org/external/datamapper/api/v1/NGDP_RPCH?periods=2024).
This data is stored in Kafka under the topic qixshawnchen_econ_data_final. Afterward, a table for this data is created in HBase using the following HBase shell command:

 ‘hbase shell’ and ‘create qixshawnchen_final_project_speed_hbase, info’. 
The data stored in Kafka is then transferred to HBase by applying a Scala JAR file located at:
/home/sshuser/qixshawnchen/final_project/speed_layer/target/uber-speedlayertest1-1.0-SNAPSHOT.jar.
The command to execute the Scala JAR file is:
“spark-submit \
  --master local[2] \
  --driver-java-options "-Dlog4j.configuration=file:///home/hadoop/ss.log4j.properties" \
  --class KafkaToHBase \
  uber-speedlayertest1-1.0-SNAPSHOT.jar \
  $KAFKABROKERS”

  ![Alt text](./readMe3.png "Speed Layer: Year of 2024")

Service Layer:
The service layer comprises the following components:
•	app.js: Backend logic.
•	index.html: Main interface for user interaction.
•	result.mustache: Template for displaying query results.
Users can access worldwide economic data by selecting a country and specifying a year. Data for years prior to 2024 is retrieved from the batch layer, while real-time data for 2024 is accessed via the speed layer.


