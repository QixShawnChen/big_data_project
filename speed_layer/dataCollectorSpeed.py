import requests
import time
import csv
import subprocess
from io import StringIO
from kafka import KafkaProducer
import json
import datetime



#https://www.imf.org/external/datamapper/api/v1/NGDP_RPCH?periods=2024


# Define country ID list and mapping to country names
country_id_list = ['AFG', 'ARG', 'ABW', 'AUS', 'BLZ', 'BRA', 'CAN', 'CHN', 'DNK', 'EGY', 'BEL',
                   'FRA', 'DEU', 'IND', 'IDN', 'ISR', 'ITA', 'JPN',
                   'MYS', 'NZL', 'NOR', 'PER', 'PHL', 'ROU', 'ZAF',
                   'ESP', 'SWE', 'CHE', 'PER', 'THA', 'ARE', 'GBR',
                   'USA', 'VNM', 'ZMB', 'ZWE'
                   ]

country_name_dict = {
    'AFG': 'Afghanistan',
    'ARG': 'Argentina',
    'ABW': 'Aruba',
    'AUS': 'Australia',
    'BLZ': 'Belize',
    'BRA': 'Brazil',
    'CAN': 'Canada',
    'CHN': 'China',
    'DNK': 'Denmark',
    'EGY': 'Egypt',
    'BEL': 'Belgium',
    'FRA': 'France',
    'DEU': 'Germany',
    'IND': 'India',
    'IDN': 'Indonesia',
    'ISR': 'Israel',
    'ITA': 'Italy',
    'JPN': 'Japan',
    'MYS': 'Malaysia',
    'NZL': 'New Zealand',
    'NOR': 'Norway',
    'PER': 'Peru',
    'PHL': 'Philippines',
    'ROU': 'Romania',
    'ZAF': 'South Africa',
    'ESP': 'Spain',
    'SWE': 'Sweden',
    'CHE': 'Switzerland',
    'THA': 'Thailand',
    'ARE': 'United Arab Emirates',
    'GBR': 'United Kingdom',
    'USA': 'United States',
    'VNM': 'Vietnam',
    'ZMB': 'Zambia',
    'ZWE': 'Zimbabwe'
}

indicator_list = [
    'NGDP_RPCH', 'PCPIPCH', 'PPPPC', 'PPPGDP', 'LP', 'BCA', 'LUR',
    'rev', 'GGXCNL_NGDP', 'NGS_GDP', 'GGXCNL_GDP', 'NI_GDP'
]


kafka_broker = "wn0-kafka.m0ucnnwuiqae3jdorci214t2mf.bx.internal.cloudapp.net:9092,wn1-kafka.m0ucnnwuiqae3jdorci214t2mf.bx.internal.cloudapp.net:9092" 
kafka_topic = "qixshawnchen_econ_data_final"  

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=kafka_broker,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

#rows = [["indicator", "country_id", "country_name", "year", "value"]]

# Initialize structured data
final_data = {}

for indicator in indicator_list:
    url = f"https://www.imf.org/external/datamapper/api/v1/{indicator}?periods=2024"
    print(f"Fetching data for Indicator: {indicator}...")
    try:
        time.sleep(5)  # Pause to respect API rate limits
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            values = data.get('values', {}).get(indicator, {})
            
            # Create a nested dictionary for this indicator
            if indicator not in final_data:
                final_data[indicator] = {}
            
            for country_id, year_data in values.items():
                if country_id in country_id_list:
                    value_2024 = year_data.get('2024')  # Directly fetch 2024 data
                    if value_2024 is not None:
                        country_name = country_name_dict.get(country_id, "Unknown")
                        final_data[indicator][country_id] = {
                            "country_name": country_name,
                            "year": "2024",
                            "value": value_2024
                        }
        else:
            print(f"Failed to fetch data for Indicator: {indicator}, Status Code: {response.status_code}")
        
    except Exception as e:
        print(f"An error occurred while fetching data for {indicator}: {e}")


# Add timestamps
timestamp_iso = datetime.utcnow().isoformat() + "Z"  
timestamp_seconds = int(time.time())  

# Add timestamps to the final JSON
final_json_with_time = {
    "timestamp_second": timestamp_seconds,
    "timestamp": timestamp_iso,
    "data": final_data
}

# Convert to JSON and send to Kafka
producer.send(kafka_topic, value=final_json_with_time)
print(f"Data sent to Kafka topic: {kafka_topic} with timestamps {timestamp_iso} and {timestamp_seconds}")

producer.flush()
producer.close()
