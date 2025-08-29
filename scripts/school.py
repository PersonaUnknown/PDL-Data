import asyncio
import os
from dotenv import load_dotenv
load_dotenv()

from peopledatalabs import PDLPY
api_key = os.getenv("PEOPLE_DATA_LABS_API_KEY")
CLIENT = PDLPY(api_key=api_key)

import csv
school_csv = "../original_data/us-colleges-and-universities.csv"
output_csv = "../parsed_data/us-colleges-and-universities.csv"

def parseCollegeCSV():
    """
    Reads the CSV file set by school_csv, then creates a new
    CSV file containing the following information:
    {
        display_name: human readable string of school,
        pdl_name: how People Data Labs stores the school,
        website: "N/A" or college website,
        type: "secondary" or "post-secondary"
    }
    """
    new_csv = []
    field_names = ["display_name", "pdl_name", "website", "type"]
    # Read CSV and compile colleges / universities
    with open(school_csv, 'r', encoding="utf-8") as file:
        dict_reader = csv.DictReader(file)
        for row in dict_reader:
            name = row.get("NAME", "NOT AVAILABLE")
            display_name = name.title()
            pdl_name = name.lower()
            website = row.get("WEBSITE", "NOT AVAILABLE")
            new_csv.append({
                "display_name": display_name,
                "pdl_name": pdl_name,
                "website": website,
                "type": "post-secondary"
            })
    # Save everything to a new CSV
    with open(output_csv, 'w', newline='', encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=field_names)
        writer.writeheader()
        writer.writerows(new_csv)

async def doubleCheckSchools():
    """
    Read the output CSV file and write down a list of schools whose
    guessed pdl_name does not match whatever gets outputted by PDL Python SDK
    """
    invalidSchools = []
    with open("../parsed_data/us-colleges-and-universities.csv", 'r', encoding="utf-8") as file:
        dict_reader = csv.DictReader(file)
        for row in dict_reader:
            display_name = row.get("display_name")
            pdl_name = row.get("pdl_name")
            PARAMS = {
                "name": display_name
            }
            print(f'Checking: {display_name}')
            response = CLIENT.school.cleaner(**PARAMS)
            json = response.json()
            status = response.status_code
            if status != 200:
                invalidSchools.append(f'{display_name} : dne\n')
                continue
            actual_name = json["name"]
            if pdl_name != actual_name:
                invalidSchools.append(f'{display_name} : {actual_name}\n')
            else:
                print(f'{pdl_name} is correct')
    with open("../parsed_data/invalid-schools.txt", 'w', encoding="utf-8") as file:
        file.writelines(invalidSchools)

asyncio.run(doubleCheckSchools())