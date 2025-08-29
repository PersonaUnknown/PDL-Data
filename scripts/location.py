import csv
location_csv = "../original_data/uscities.csv"
output_csv = "../parsed_data/uscities.csv"

def parseLocationCSV():
    """
    Reads the CSV file set by location_csv, then creates a new
    CSV file containing the following information:
    {
        display_name: human readable string of location,
        pdl_name: how People Data Labs stores the location,
        type: "city" or "state"
    }
    pdl_name is formatted like this: san francisco, california, united states
    """
    new_csv = []
    states = set()
    field_names = ["display_name", "pdl_name", "type"]
    # Read CSV and compile cities + states
    with open(location_csv, 'r', encoding="utf-8") as file:
        dict_reader = csv.DictReader(file)
        for row in dict_reader:
            city = row.get("city", "N/A")
            state = row.get("state_name", "N/A")
            states.add(state)
            region = "united states" if state != "Puerto Rico" else "puerto rico"
            display_name = f'{city}, {state}'
            pdl_name=f'{city.lower()}, {state.lower()}, {region.lower()}'
            new_csv.append({
                "display_name": display_name,
                "pdl_name": pdl_name,
                "type": "city"
            })
    # Compile just states
    for state in states:
        display_name = f'{state}, USA' if state != "Puerto Rico" else state 
        pdl_name = f'{state.lower()}, united states' if state != "Puerto Rico" else state.lower()
        new_csv.append({
            "display_name": display_name,
            "pdl_name": pdl_name,
            "type": "state"
        })
    # Save everything to a new CSV
    with open(output_csv, 'w', newline='', encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=field_names)
        writer.writeheader()
        writer.writerows(new_csv)

parseLocationCSV()