from datetime import datetime
import random
import sys
import json
import requests

def generate_metadata(disaster_type):
    if disaster_type == "flood":
        return {
            "rainfall_mm": round(random.uniform(50, 300), 1),
            "water_level_m": round(random.uniform(1, 8), 1),
            "wind_speed_kmh": round(random.uniform(5, 40), 1),
            "temperature_c": round(random.uniform(20, 35), 1)
        }
    elif disaster_type == "earthquake":
        return {
            "rainfall_mm": 0.0,
            "water_level_m": 0.0,
            "wind_speed_kmh": round(random.uniform(0, 20), 1),
            "temperature_c": round(random.uniform(15, 40), 1)
        }
    elif disaster_type == "cyclone":
        return {
            "rainfall_mm": round(random.uniform(80, 400), 1),
            "water_level_m": round(random.uniform(2, 10), 1),
            "wind_speed_kmh": round(random.uniform(60, 200), 1),
            "temperature_c": round(random.uniform(20, 32), 1)
        }
    elif disaster_type == "fire":
        return {
            "rainfall_mm": 0.0,
            "water_level_m": 0.0,
            "wind_speed_kmh": round(random.uniform(5, 30), 1),
            "temperature_c": round(random.uniform(35, 50), 1)
        }
    else:  # infrastructure
        return {
            "rainfall_mm": round(random.uniform(0, 50), 1),
            "water_level_m": round(random.uniform(0, 2), 1),
            "wind_speed_kmh": round(random.uniform(0, 30), 1),
            "temperature_c": round(random.uniform(15, 40), 1)
        }

    
count = int(sys.argv[1]) if len(sys.argv) > 1 else 10

flood_events = [
    "A severe flood has occurred in {location}, causing widespread damage and displacement of residents .",
    "Heavy rainfall has led to flooding in {location}, resulting in road closures and property damage.",
    "The river has overflowed its banks, causing flooding in nearby neighborhoods and affecting local businesses in {location}.",
    "Flash floods have been reported in {location}, prompting emergency response and evacuation efforts.",
    "The local authorities have issued flood warnings for {location} due to rising water levels and potential hazards.",
    "Residents of {location} are advised to take precautions and stay informed about the flood situation."
]

fire_events = [
    "A wildfire has broken out in {location}, threatening homes and wildlife in the area.",
    "Firefighters are battling a large fire in {location}, with efforts focused on containment and evacuation.",
    "The fire in {location} has caused significant damage to property and natural habitats, prompting emergency response.",
    "Authorities have issued evacuation orders for residents in {location} due to the spreading fire.",
    "The wildfire in {location} has been fueled by dry conditions and strong winds, posing a serious threat to the community.",
    "Local authorities are urging residents of {location} to stay alert and follow safety guidelines during the ongoing fire emergency."
]

earthquake_events = [
    "A powerful earthquake has struck {location}, causing widespread destruction and casualties.",
    "The earthquake in {location} has resulted in collapsed buildings and infrastructure damage, prompting rescue operations.",
    "Residents of {location} are advised to stay indoors and follow safety protocols after the recent earthquake.",
    "Emergency services are responding to the aftermath of the earthquake in {location}, providing aid and support to affected communities.",
    "The earthquake in {location} has triggered aftershocks, leading to further concerns about structural stability and safety in the area.",
    "Authorities are assessing the impact of the earthquake in {location} and coordinating relief efforts for those affected by the disaster."
]

cyclone_events = [
    "A severe cyclone has made landfall in {location}, causing extensive damage and power outages.",
    "Residents of {location} are being evacuated as the cyclone approaches, with emergency shelters being set up.",
    "The cyclone in {location} has brought heavy rainfall and strong winds, leading to flooding and property damage.",
    "Authorities are closely monitoring the cyclone's path and issuing warnings to residents in {location} to stay safe.",
    "The impact of the cyclone in {location} has disrupted transportation and communication networks, affecting daily life.",
    "Emergency response teams are working tirelessly to assist those affected by the cyclone in {location}, providing aid and support."
]

infrastructure_events = [
    "A major infrastructure failure has occurred in {location}, leading to disruptions in essential services.", 
    "The failure in {location} has resulted in water supply issues and transportation delays.",
    "Residents of {location} are experiencing difficulties due to the infrastructure failure, with essential services being affected.",
    "Emergency services are working to restore the infrastructure in {location} and minimize the impact on the community.",
    "The infrastructure failure in {location} has prompted a review of maintenance protocols and safety measures.",
    "Efforts are underway to repair the damaged infrastructure in {location} and ensure the safety of residents."

]

disaster_type = ["flood","fire","earthquake","cyclone","infrastructure"]
location=[
    {"location":"Mumbai","latitude":19.0760,"longitude":72.8777},
    {"location":"Delhi","latitude":28.7041,"longitude":77.1025},
    {"location":"Bangalore","latitude":12.9716,"longitude":77.5946},
    {"location":"Chennai","latitude":13.0827,"longitude":80.2707},
    {"location":"Kolkata","latitude":22.5726,"longitude":88.3639},
    {"location":"Hyderabad","latitude":17.3850,"longitude":78.4867},
    {"location":"Pune","latitude":18.5204,"longitude":73.8567},
    {"location":"Ahmedabad","latitude":23.0225,"longitude":72.5714},
    {"location":"Jaipur","latitude":26.9124,"longitude":75.7873},
    {"location":"Lucknow","latitude":26.8467,"longitude":80.9462}
]
sources=['Citizen Report','Government Alert','News Agency','Social Media','Emergency Services','sensor','Weather Station','Satellite Imagery','Local Authority','Community Organization']



severity=["low","medium","high","critical"]

for i in range(count):
    event_id=f"event_{i+1}"
    disaster=random.choice(disaster_type)
    metadata=generate_metadata(disaster)
    timestamp=datetime.now().isoformat()
    loca=random.choice(location)
    loc=loca["location"]
    lat=loca["latitude"]    
    lon=loca["longitude"]
    if disaster=="flood":
            text=random.choice(flood_events).format(location=loc)
    elif disaster=="fire":
            text=random.choice(fire_events).format(location=loc)
    elif disaster=="earthquake":
            text=random.choice(earthquake_events).format(location=loc)
    elif disaster=="cyclone":
            text=random.choice(cyclone_events).format(location=loc)
    elif disaster=="infrastructure":
            text=random.choice(infrastructure_events).format(location=loc)
    sev=random.choice(severity)
    source=random.choice(sources)
    
    event={
        "event_id":event_id,
        "timestamp":timestamp,
        "text":text,
        "location":loc,
        "disaster_type":disaster,
        "latitude":lat,
        "longitude":lon,
        "severity":sev,
        "source":source,
        "metadata":metadata
    }

    response=requests.post("http://localhost:8000/events",json=event)
    print(f"Status: {response.status_code} -> {response.json()}")

  

    

