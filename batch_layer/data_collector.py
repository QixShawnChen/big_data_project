import requests
import time
import csv
import subprocess
from io import StringIO

"""
Country Code List:
Afghanistan: AFG
Argentina: ARG
Aruba: ABW
Australia: AUS
Belize: BLZ
Brazil: BRA
Canada: CAN
China: CHN
Denmark: DNK
Egypt: EGY
Belgium: BEL
France: FRA	
Germany: DEU
India: IND
Indonesia: IDN
Israel: ISR
Italy: ITA
Japan: JPN
Malaysia: MYS
Mexico: MEX
New Zealand: NZL
Norway: NOR
Peru: PER
Philippines (the): PHL
Romania: ROU
South Africa: ZAF
Spain: ESP
Sweden: SWE
Switzerland: CHE
Thailand: THA
United Arab Emirates (the): ARE
United Kingdom of Great Britain and Northern Ireland (the): GBR
United States of America (the): USA
Viet Nam: VNM
Zambia: ZMB
Zimbabwe: ZWE
"""

"""
"NGDP_RPCH":"label":"Real GDP growth"
"PCPIPCH":"label":"Inflation rate, average consumer prices"
"PPPPC":"label":"GDP per capita, current prices"
"PPPGDP":"label":"GDP, current prices"
"LP":"label":"Population"
"BCA":"label":"Current account balance U.S. dollars"
"LUR":"label":"Unemployment rate"
"rev":"label":"Government revenue, percent of GDP"
"GGXCNL_NGDP":"label":"General government net lending\/borrowing"
"NGS_GDP":"label":"Gross National Savings (% of GDP)"
"NI_GDP":"label":"Total Investment (% of GDP)"
"GGXCNL_GDP":"label":"Overall Fiscal Balance, Including Grants (% of GDP)"
"""

"""country_id_list = ['AFG', 'AFG', 'CAN', 'CHN', 'DNK', 'EGY', 'BEL',
                   'FRA', 'DEU', 'IND', 'IDN', 'ISR', 'ITA', 'JPN',
                   'MYS', 'NZL', 'NOR', 'PER', 'PHL', 'ROU', 'ZAF',
                   'ESP', 'SWE', 'CHE', 'PER', 'THA', 'ARE', 'GBR',
                   'USA', 'VNM', 'ZMB', 'ZWE'
                   ]


indicator_list = ['NGDP_RPCH', 'PCPIPCH', 'PPPPC', 'PPPGDP', 'LP', 'BCA', 'LUR', 'rev', 'GGXCNL_NGDP', 'NI_GDP', 'GGXCNL_GDP'] 

dictionary_indicator_countryid = {}

for indicator in indicator_list:
    for country_id in country_id_list:
        url = f"https://www.imf.org/external/datamapper/api/v1/INDICATOR/{indicator}/{country_id}"
        time.sleep(2)
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json() 
            print(data)
"""


"""
# Define the country and indicator lists
country_id_list = ['CAN', 'CHN']
indicator_list = ['LUR', 'rev', 'GGXCNL_NGDP']

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

#output_file = "./data_temp/data.csv"
output_file = "./data.csv"
rows = []

for country_id in country_id_list:
    for indicator in indicator_list:
        url = f"https://www.imf.org/external/datamapper/api/v1/INDICATOR/{indicator}/{country_id}"
        print(f"Fetching data for Country: {country_id}, Indicator: {indicator}...")
        time.sleep(5)  
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            values = data.get('values', {}).get(indicator, {}).get(country_id, {})
            for year, value in values.items():
                country_name = country_name_dict[country_id]
                rows.append([indicator, country_id, country_name, year, value])
        else:
            print(f"Failed to fetch data for {country_id}, {indicator}: {response.status_code}")

with open(output_file, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["indicator", "country_id", "country_name", "year", "value"])  
    writer.writerows(rows)

print(f"Data successfully written to {output_file}.")
"""




import requests
import time
import csv
import subprocess
from io import StringIO



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


indicator_list = ['NGDP_RPCH', 'PCPIPCH', 'PPPPC', 'PPPGDP', 'LP', 'BCA', 'LUR', 
                  'rev', 'GGXCNL_NGDP', 'NGS_GDP', 'GGXCNL_GDP', 'NI_GDP'] 


hdfs_output_path = "/tmp/qixshawnchen/final_project/final_data.csv"

rows = [["indicator", "country_id", "country_name", "year", "value"]]

for country_id in country_id_list:
    for indicator in indicator_list:
        url = f"https://www.imf.org/external/datamapper/api/v1/INDICATOR/{indicator}/{country_id}"
        print(f"Fetching data for Country: {country_id}, Indicator: {indicator}...")
        time.sleep(5)  
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            values = data.get('values', {}).get(indicator, {}).get(country_id, {})
            for year, value in values.items():
                country_name = country_name_dict[country_id]
                rows.append([indicator, country_id, country_name, year, value])
        else:
            print(f"Failed to fetch data for {country_id}, {indicator}: {response.status_code}")

csv_buffer = StringIO()
writer = csv.writer(csv_buffer)
writer.writerows(rows)
csv_data = csv_buffer.getvalue()
csv_buffer.close()

process = subprocess.Popen(
    ["hdfs", "dfs", "-put", "-f", "-", hdfs_output_path],
    stdin=subprocess.PIPE,
    stderr=subprocess.PIPE
)
stdout, stderr = process.communicate(input=csv_data.encode())

# Check for success or failure
if process.returncode == 0:
    print(f"Data successfully written to HDFS at {hdfs_output_path}")
else:
    print(f"Failed to write data to HDFS. Error: {stderr.decode()}")




"""
country_id_list = ['AFG', 'AFG', 'CAN', 'CHN']
indicator_list = ['LUR', 'rev', 'GGXCNL_NGDP']


dictionary_indicator_countryid = {}

# Loop through countries and indicators
for country_id in country_id_list:
    if country_id not in dictionary_indicator_countryid:
        dictionary_indicator_countryid[country_id] = {}
    
    for indicator in indicator_list:
        url = f"https://www.imf.org/external/datamapper/api/v1/INDICATOR/{indicator}/{country_id}"
        time.sleep(5)  
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            values = data.get('values', {}).get(indicator, {}).get(country_id, {})
            dictionary_indicator_countryid[country_id][indicator] = values
        else:
            print(f"Failed to fetch data for {country_id}, {indicator}: {response.status_code}")

print(dictionary_indicator_countryid)
"""