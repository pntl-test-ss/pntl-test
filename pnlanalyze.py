import requests
import json
import pandas as pd
from datetime import datetime

# Function to fetch JSON data from the given API URL and convert it to a DataFrame
def fetch_url_data(api_url, params=None):
    response = requests.get(api_url, params=params)
    if response.status_code == 200:
        df = pd.DataFrame(response.json())
        return df
    else:
        print(f"Error fetching data from {api_url}. Status code: {response.status_code}")
        return None

# Function to fetch JSON data from local storage and convert it to a DataFrame
def fetch_local_data(data_path):
    with open(data_path, "r") as f:
        data = json.load(f)
    df = pd.DataFrame(data)
    return df

# URLs for events and weather data
events_url = "https://www.pingtt.com/exam/events"
weather_url = "https://www.pingtt.com/exam/weather"

# Prompt the user for the path to the json files
root_path = input("Enter the path to where your employees and attendance json files are stored on your machine: ")

if root_path and not root_path.endswith('\\'):
    root_path += '\\'

employee_data_path = f"{root_path}employees.json"
attendance_data_path = f"{root_path}attendance.json"

print("Fetching employee and attendance data...")
# Fetch employee and attendance data from local storage
employee_data = fetch_local_data(employee_data_path)
attendance_data = fetch_local_data(attendance_data_path)
print("Completed!")

print("Fetching events and weather data...")
# Fetch events and weather data from URLs
events_data = fetch_url_data(events_url)
weather_data = fetch_url_data(weather_url)
print("Completed!")

# Filter the data for the year 2023
events_data['event_date'] = pd.to_datetime(events_data['event_date'], format='%Y-%m-%d')
weather_data['date'] = pd.to_datetime(weather_data['date'], format='%Y-%m-%d')
attendance_data['date'] = pd.to_datetime(attendance_data['date'], format='%Y-%m-%d')
events_data = events_data[events_data['event_date'].dt.year == 2023]
weather_data = weather_data[weather_data['date'].dt.year == 2023]
attendance_data = attendance_data[attendance_data['date'].dt.year == 2023]

print("\nIs record_id in employee data unique?")
if not employee_data.duplicated(subset=['record_id']).any():
    print("Yes. record_id in employee data is unique.\n")
    print("Does each record_id in employee_data uniquely identify an employee?")
    if not employee_data.duplicated(subset=['name', 'email_address']).any():
        print("Yes. Each record_id uniquely identifies an employee.\n")
    else:
        print("No. There are multiple record_id's for the same employee. <-- Data must be sanitized.\n")
else:
    print("No. There must be some other way to uniquely identify employees.\n")

print("Does each record_id in attendance_data uniquely identifies an employee's attendance record?")
if not attendance_data.duplicated(subset=['employee_record_id', 'date']).any():
    print("Yes. Each id record_id in attendance_data uniquely identify an employee's attendance record.\n")
else:
    print("No. There are multiple id's for the same attendance record. <-- Data must be sanitized.\n")

print("Is there a record in attendance data for every employee on each weekday of 2023?")
# Create a DataFrame with every weekday in 2023
weekdays_2023_range = pd.date_range(start='2023-01-01', end='2023-12-31', freq='B')
weekdays_2023 = pd.DataFrame({'date': weekdays_2023_range})
weekdays_2023['key'] = 1 # Create a key column with the same constant value in both DataFrames
employee_data['key'] = 1

# Join employee_data with  weekdays_2023 <-- This gives us a record for each employee for each weekday in 2023
employees_weekdays_2023 = pd.merge(employee_data, weekdays_2023)
employees_weekdays_2023 = employees_weekdays_2023.drop('key', axis=1)

# Join employees_weekdays_2023 with attendance_data on date and employee_id keeping all data on the left <-- This gives us a null on the right if the employee_id does not exist on the right
employees_weekdays_2023['date'] = pd.to_datetime(employees_weekdays_2023['date'], format='%Y-%m-%d')
attendance_data['date'] = pd.to_datetime(attendance_data['date'], format='%Y-%m-%d')
employees_weekdays_2023_attendance = pd.merge(employees_weekdays_2023, attendance_data, left_on=['date', 'record_id'], right_on=['date', 'employee_record_id'], how='left')
missing_emp_wday_2023_att = employees_weekdays_2023_attendance[employees_weekdays_2023_attendance['record_id_y'].isnull()]

if missing_emp_wday_2023_att.empty:
    print("Yes. There is a record in attendance data for every employee on each weekday of 2023 <-- There must be a different way to test for absences from checking for missing records.\n")
else:
    print("No. There are missing records for some employees on some weekdays of 2023. <-- A missing record must indicate an absence.\n")

print("Are there null clocks in attendance data?")
case_1 = attendance_data[(attendance_data['clock_in'].isnull()) & (~attendance_data['clock_out'].isnull())]
case_2 = attendance_data[(~attendance_data['clock_in'].isnull()) & (attendance_data['clock_out'].isnull())]
case_3 = attendance_data[(attendance_data['clock_in'].isnull()) & (attendance_data['clock_out'].isnull())]

if not case_1.empty:
    print("There are null clock ins with non-null clock-outs. <-- Handle these when checking for late clock ins.\n")
else:
    print("No null clock ins with non-null clock-outs")
if not case_2.empty:
    print("There are null clock outs with non-null clock-ins. <-- Handle these when checking for early clock outs.\n")
else:
    print("No null clock outs with non-null clock-ins")
if not case_3.empty:
    print("There are null clock outs with null clock-ins. <-- These would be the absences.\n")
else:
    print("No null clock outs with null clock-ins. <-- There must be a different way to test for absences from checking for null clock ins and outs.\n")

print("Is id a unique identifier for events?")
if not events_data.duplicated(subset=['id']).any():
    print("Yes. ID is a unique identifier for events.\n")
    print("Does each id in events_data uniquely identify an event?")
    if not events_data.duplicated(subset=['event_name', 'event_date', 'country']).any():
        print("Yes. Each id uniquely identifies an event.\n")
    else:
        print("No. There are multiple id's for the same event. <-- Data must be sanitized.\n")
else:
    print("No. There must be some other way to uniquely identify events.\n")

print("Are there events two days or fewer apart in the same country in event data?")
events_data['event_date'] = pd.to_datetime(events_data['event_date'], format='%Y-%m-%d') # Convert 'event_date' to datetime type
events_data = events_data.sort_values(by=['country', 'event_date']) # Sort DataFrame based on 'country' and 'event_date'
consecutive_dates = events_data.groupby('country')['event_date'].diff().le(pd.Timedelta(days=2)) # Check for consecutive dates in the same country

if consecutive_dates.any():
    print("Yes. There are events two days or fewer apart in the same country. <-- Handle multiple possible events for the same clock date.\n")
else:
    print("No. There are no events two days or fewer apart in the same country. <-- Each clock date represents a single event.\n")

print("Is there a record in weather data for each weekday of 2023?")
# Join weekdays_2023 with weather_data on date all data on the left <-- This gives us a null on the right if the date does not exist on the right
weekdays_2023_weather = pd.merge(weekdays_2023, weather_data, on='date', how='left')
missing_wday_2023_wthr = weekdays_2023_weather[weekdays_2023_weather['id'].isnull()]

if missing_emp_wday_2023_att.empty:
    print("Yes. There is a record in weather data for each weekday of 2023 <-- There must be a column to test for good weather.\n")
else:
    print("No. There are missing records for weather on some weekdays of 2023. <-- Assume good weather on days without weather data.\n")

print("Is date&country a unique identifier for weather?")
if not weather_data.duplicated(subset=['date', 'country']).any():
    print("Yes. date&country is a unique identifier for weather. <-- Use date&country to test for weather conditions.")
else:
    print("No. There are multiple weather records for the same day in the same country. <-- Handle multiple possible weather conditions per day.")
