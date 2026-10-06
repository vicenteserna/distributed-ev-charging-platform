import time
import requests
import os
import sys
import random

import argparse

# Add parent directory to path to import ev_common
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ev_common.config import PORT_CENTRAL

CENTRAL_URL = f"http://ev_central:{PORT_CENTRAL}"
if os.getenv('DB_HOST') is None:
    CENTRAL_URL = f"http://localhost:{PORT_CENTRAL}"

CITY = "Madrid" # Monitor Madrid for now

def get_weather(city, fixed_temp=None):
    if fixed_temp is not None:
        return fixed_temp
    
    api_key = os.getenv('OPENWEATHER_API_KEY')
    if not api_key:
        # Fallback to mock if no key provided
        print(f"No API Key found. Using mock data for {city}.")
        temp = random.uniform(-5, 30)
        print(f"Mock weather in {city}: {temp:.2f}ºC")
        return temp

    try:
        resp = requests.get(
            "https://api.openweathermap.org/data/2.5/weather",
            params={"q": city, "appid": api_key, "units": "metric"},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            temp = data['main']['temp']
            print(f"OpenWeatherMap {city}: {temp:.2f}ºC")
            return temp
        else:
            print(f"OpenWeatherMap returned status {resp.status_code} for {city}")
            return None
    except (requests.RequestException, KeyError, TypeError, ValueError) as e:
        print(f"Weather API connection error: {e}")
        return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--city', default=CITY)
    parser.add_argument('--temp', type=float, help='Force a specific temperature')
    args = parser.parse_args()

    print(f"Starting EV_Weather Monitor...")
    
    # If manual city provided, just monitor that.
    # Otherwise, fetch from Central.
    
    while True:
        try:
            cities_to_check = []
            if args.city != CITY: # User provided a specific city
                cities_to_check = [args.city]
            else:
                # Fetch from Central
                try:
                    resp = requests.get(f"{CENTRAL_URL}/locations", timeout=5)
                    if resp.status_code == 200:
                        cities_to_check = resp.json().get('locations', [])
                except requests.RequestException:
                    print("Could not fetch locations from Central. Defaulting to Madrid.")
                    cities_to_check = [CITY]
            
            if not cities_to_check:
                cities_to_check = [CITY]

            for city in cities_to_check:
                temp = get_weather(city, args.temp)
                if temp is None:
                    continue
                
                # Send update to Central
                try:
                    requests.post(f"{CENTRAL_URL}/update_weather", json={'city': city, 'temp': temp}, timeout=5)
                except requests.RequestException as exc:
                    print(f"Could not update weather for {city}: {exc}")

                if temp < 0:
                    print(f"ALERT: Low temperature in {city} ({temp:.2f}ºC). Sending alert...")
                    try:
                        resp = requests.post(f"{CENTRAL_URL}/alert", json={'city': city, 'type': 'LOW_TEMP'}, timeout=5)
                        print(f"Alert sent: {resp.status_code}")
                    except Exception as e:
                        print(f"Failed to send alert: {e}")
                else:
                    print(f"Weather OK in {city} ({temp:.2f}ºC)")
            
            if args.temp is not None:
                break # Run once if manual temp provided
                
            time.sleep(10) 
        except Exception as e:
            print(f"Error in weather loop: {e}")
            time.sleep(5)

if __name__ == '__main__':
    main()
