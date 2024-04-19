import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = "log_file.txt"
dataset_file = "transformed_data.csv"

def extract_from_csv(csv_file):
    dataframe = pd.read_csv(csv_file)
    return dataframe

def extract_from_xml(xml_file):
    dataframe = pd.DataFrame(columns=["name", "height", "weight"])
    parsed_file = ET.parse(xml_file)
    root = parsed_file.getroot()

    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        dataframe = pd.concat([dataframe, pd.DataFrame([{"name":name, "height":height, "weight":weight}])], ignore_index=True)
    return dataframe

def extract_from_json(json_file):
    dataframe = pd.read_json(json_file, lines=True)
    return dataframe

def extract():
    extracted_data = pd.DataFrame(columns=["name", "height", "weight"])

    #loop thru all .csv files
    for csv in glob.glob("*.csv"):
        if "source" in csv:
            extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csv))], ignore_index=True)
    
    #loop thru all .json files
    for json in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(json))], ignore_index=True)
    
    for xml in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xml))], ignore_index=True)
    
    return extracted_data

def transform(data):
    """
        Convert inches to meters and round off to two decimals
        1 inch  = 0.0254 meters
    """
    data['height'] = round(data.height * 0.0254,2)

    """
        Convert pounds to kilograms and round off to two decimals
        1 pound = 0.45359237 kilograms
    """
    data['weight'] = round(data.weight * 0.45359237, 2)
    return data

def load_data(transformed_data,target_file_path):
    transformed_data.to_csv(target_file_path, index = False)

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    current_time = datetime.now() # get current timestamp 
    timestamp = current_time.strftime(timestamp_format)
    with open(log_file, "a") as logging_file:
        logging_file.write(timestamp + ',' + message + '\n')

#El log agrega timestamps en el mismo archivo cada vez que se corre el script, acumulandolos.
log_progress("ETL Job started")

log_progress("Extract phase started")
data = extract()
log_progress("Extract phase ended")

log_progress("Transform phase started")
transformed_data = transform(data)

log_progress("Transform phase ended")

log_progress("Load phase started")
load_data(transformed_data, 'output.csv') #Se puede mejorar haciendo que el archivo excel se cree de nuevo cada vez que se corre el script
log_progress("Load phase ended")

log_progress("ETL Job ended")


