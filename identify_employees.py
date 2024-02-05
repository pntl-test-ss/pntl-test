"""
Script Name: identifies_employees.py
Description: This script generates a list of employees (results.json) in 2023 who show a pattern of tardiness/leaving early or absenteeism on the day of, day after,
or day before an event in their country with the exception of days with extreme weather. A pattern is determined as having occurred for > 3 events for the year.
Note that an employee does not incur more than one infraction for the same clock date or the same event. The results also includes the list of events the employee
may have possibly attended.

The approach is to join datasets on common keys and filter for conditions that match behavioral pattern criteria. (The resulting dataset is printed to drilldown.csv
for manual double-checking of results.) The resulting dataset is then grouped by attendance date and then by event to count the number of events an employee attended.

test_results.py along with some manual checking in drilldown.csv can be used to double-check the results.

Author: S. S.
Created: January 3, 2024
Last Modified: January 3, 2024

Usage:
- Ensure you have Python 3.12 installed.
- Run the script: python identifies_employees.py
"""

import requests
import json
import pandas as pd
import numpy as np
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

# Add the previous and following clock dates to the events data <-- This gives us clock dates before, after and of an event
# Clock dates will be duplicated for events less than two days apart <-- Keep this in mind when counting infractions
events_data.rename(columns={'id': 'event_id'}, inplace=True) # Rename unique identifier for events to event_id
events_data['clock_date'] = events_data['event_date'] # Add clock date and set to event date
previous_day = events_data.copy()
previous_day['clock_date'] = previous_day['event_date'] - pd.to_timedelta(1, unit='d') # Add clock date and set to day before event date
next_day = events_data.copy()
next_day['clock_date'] = next_day['event_date'] + pd.to_timedelta(1, unit='d') # Add clock date and set to day after event date
events_data = pd.concat([events_data, previous_day, next_day], ignore_index=True)
events_data = events_data[events_data['clock_date'].dt.year == 2023] # Filter in case of Old Year's and New Year's events

# Join events_data and weather_data on event_date and country and filter out weekend dates and dates where there was hail, blizzard, thunderstorm or hurricane weather <-- This gives us the weekdays before, after and of an event with good weather
events_weather = pd.merge(events_data, weather_data, left_on=['clock_date', 'country'], right_on=['date', 'country'])
exclude_values = ['hail', 'blizzard', 'thunderstorm', 'hurricane']
events_weather = events_weather[
    (~events_weather['condition'].isin(exclude_values)) &
    (events_weather['max_temp'] <= 40) &
    (events_weather['clock_date'].dt.dayofweek < 5)
]

# Calculate average hours worked per week for each employee and add as a column in attendance data <-- This detail is needed in the final result
attendance_data['clock_in'] = pd.to_datetime(attendance_data['clock_in'], format='%H:%M:%S') # Convert 'clock_in' column to datetime object
attendance_data['clock_out'] = pd.to_datetime(attendance_data['clock_out'], format='%H:%M:%S') # Convert 'clock_out' column to datetime object
attendance_data['total_seconds_worked'] = (attendance_data['clock_out'] - attendance_data['clock_in']).dt.total_seconds() # Calculate total seconds worked for each record
attendance_data['average_hours_per_week'] = attendance_data.groupby('employee_record_id')['total_seconds_worked'].transform('sum') / 3600 / 52 # Group by employee id to calculate weekly average for each employee

# Join employee_data and attendance_data on employee_id and filter for late clock ins and early clock outs and absences <-- This gives us all problematic clocks and absences and the employee details needed for final result
employee_attendance = pd.merge(employee_data, attendance_data, left_on=['record_id'], right_on=['employee_record_id'])
employee_attendance['clock_in'] = pd.to_datetime(employee_attendance['clock_in'], format='%H:%M:%S') # Convert 'clock_in' column to datetime object
employee_attendance['clock_out'] = pd.to_datetime(employee_attendance['clock_out'], format='%H:%M:%S') # Convert 'clock_out' column to datetime object
employee_attendance = employee_attendance[
    (employee_attendance['clock_in'].dt.time > pd.to_datetime('08:15:00', format='%H:%M:%S').time()) |
    (employee_attendance['clock_out'].dt.time < pd.to_datetime('16:00:00', format='%H:%M:%S').time()) |
    (employee_attendance['clock_in'].isnull() & employee_attendance['clock_out'].isnull())
]

print("Checking for problematic employee clocks and absences on weekdays with good weather the day before after and of an event in their country...")
# Join employee_attendance with events_weather on clock_date and country <-- This give us all the problematic clocks and absences on weekdays with good weather the day before, after and of an event
problem_clocks_absences = pd.merge(employee_attendance, events_weather, left_on=['date', 'country'], right_on=['clock_date', 'country'])
problem_clocks_absences.to_csv(f"{root_path}drilldown.csv", index=False) # Print to csv for double-checking
print("Completed!")

print("Counting the number of infractions per employee..")
# Create a separate dataframe to preserve the possibly attended events of each employee
employee_events = problem_clocks_absences.groupby('record_id_x').agg({ # Group by employee id to generate a list of events for each possibly attended event
	'event_name': lambda x: list(x),
	'event_date': lambda x: list(x.dt.strftime('%Y-%m-%d')),
	'country': lambda x: list(x)
}).reset_index()
employee_events['events'] = None # Add a new column to store events in JSON format
employee_events['events'] = employee_events.apply(
    lambda row: json.loads(pd.DataFrame({
        'country': row['country'],
        'event_name': row['event_name'],
        'event_date': row['event_date']
    }).drop_duplicates().reset_index(drop=True).to_json(orient='records')), axis=1
)

# Count the number of infractions per employee (An employee should not incur more than one infraction for the same clock date or the same event)
problem_clocks_absences = problem_clocks_absences.sort_values(by=['record_id_x', 'clock_date', 'event_id']) # Sort so that 'first' aggregation function behavior is predictable
problem_clocks_absences['count_empid_evt'] = problem_clocks_absences.groupby(['record_id_x', 'event_id']).transform('size') # Count event occurrences per employee
problem_clocks_absences = problem_clocks_absences.loc[problem_clocks_absences.groupby(['record_id_x', 'clock_date'])['count_empid_evt'].idxmax()] # Flatten data by clock date to get one event (the one with max occurrences) per clock date.
problem_clocks_absences = problem_clocks_absences.groupby(['record_id_x', 'event_id']).agg({ # Flatten data by event id
    'name': 'first',
    'work_id_number': 'first',
    'email_address': 'first',
    'country': 'first',
    'phone_number': 'first',
    'average_hours_per_week': 'first',
    'clock_date': 'first', # One clock date per event
}).reset_index()
problem_clocks_absences['freq'] = 1 # Add a new column to count the infractions
problem_clocks_absences = problem_clocks_absences.groupby(['record_id_x']).agg({
    'name': 'first',
    'work_id_number': 'first',
    'email_address': 'first',
    'country': 'first',
    'phone_number': 'first',
    'average_hours_per_week': 'first',
    'freq': 'size', # Count the infractions per employee
}).reset_index()

# Filter for employees with more than 3 infractions and join with employee_events on employee id to get all possible events for each employee
problem_freq = problem_clocks_absences[problem_clocks_absences['freq'] > 3].copy()
problem_freq = pd.merge(problem_freq, employee_events[['record_id_x', 'events']], on='record_id_x')
print("Completed!")

print(f"Printing results to {root_path}results.json...")
# Format and output results
problem_freq = problem_freq.drop(columns=['freq']) # Remove columns not in example
problem_freq.rename(columns={'record_id_x': 'record_id'}, inplace=True) # Rename columns to match example
json_data = problem_freq.to_json(orient='records')
file_path = f"{root_path}results.json"
with open(file_path, 'w') as json_file:
    json_file.write(json_data)
print("Completed!")
