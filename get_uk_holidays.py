import datetime

import requests


def map_holiday(holiday):
    try:
        return {
            "date": datetime.datetime.strptime(holiday["date"], "%Y-%m-%d").date(),
        }
    except:
        pass


data = requests.get("https://www.gov.uk/bank-holidays.json").json()
data = {
    division: sorted(
        filter(None, map(map_holiday, item.get("events", []))), key=lambda e: e["date"]
    )
    for division, item in data.items()
}
england_holidays = data["england-and-wales"]
england_holidates = [
    day["date"] for day in england_holidays if day["date"].year >= 2022
]
with open("england_holidates.py", "w") as f:
    f.write("import datetime \nengland_holidates = " + str(england_holidates))
