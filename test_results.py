"""
Script Name: test_results.py
Description:This script tests the results generated by identify_employees.py in two ways-:
1. Samples 5 rows from the results and checks that-:
   a. more than 3 events were possibly attended by each employee, and
   b. infractions happened on the days surrounding the events listed for each employee
   Note that the test is not comprehensive. For example, there is no check to verify that clock dates are only counted once towards an infraction.
2. Samples 5 rows in employee data that do not appear in the results and check that fewer than 4 problem clocks happened around event dates.
   Note that there is a many-to-many mapping between problem clocks and events, so if more than 3 problem clocks happened, use drilldown.csv
   to check exactly how many events were possibly attended, keeping in mind that an employee should not incur more than one infraction
   for the same clock date or the same event.

The approach is to filter for attendance records where the dates appear in events data (using isin) compared to
the approach in identify_employees.py to join attendance and events on date (using merge)

Author: S. S.
Created: January 3, 2024
Last Modified: January 3, 2024

Usage:
- Ensure you have Python 3.12 installed.
- Run the script: python test_results.py
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
root_path = input("Enter the path to where your employees, attendance and results json files are stored on your machine: ")
if root_path and not root_path.endswith('\\'):
    root_path += '\\'
employee_data_path = f"{root_path}employees.json"
attendance_data_path = f"{root_path}attendance.json"
results_data_path = f"{root_path}results.json"

print("Fetching employee, attendance and results data...")
# Fetch employee and attendance data from local storage
employee_data = fetch_local_data(employee_data_path)
attendance_data = fetch_local_data(attendance_data_path)
results_data = fetch_local_data(results_data_path)
print("Completed!")

print("Fetching events and weather data...")
# Fetch events and weather data from URLs
events_data = fetch_url_data(events_url)
weather_data = fetch_url_data(weather_url)
print("Completed!")

# Filter the data for the year 2023
attendance_data['clock_in'] = pd.to_datetime(attendance_data['clock_in'], format='%H:%M:%S') # Convert 'clock_in' column to datetime object
attendance_data['clock_out'] = pd.to_datetime(attendance_data['clock_out'], format='%H:%M:%S') # Convert 'clock_out' column to datetime object
attendance_data['date'] = pd.to_datetime(attendance_data['date'], format='%Y-%m-%d') # Convert 'date' column to datetime object
events_data['event_date'] = pd.to_datetime(events_data['event_date'], format='%Y-%m-%d') # Convert 'event_date' column to datetime object
weather_data['date'] = pd.to_datetime(weather_data['date'], format='%Y-%m-%d') # Convert 'date' column to datetime object
events_data = events_data[events_data['event_date'].dt.year == 2023]
weather_data = weather_data[weather_data['date'].dt.year == 2023]
attendance_data = attendance_data[attendance_data['date'].dt.year == 2023]

# Filter weather data for extreme weather and add the previous and following dates to the events data
bad_weather = weather_data[weather_data['condition'].isin(['hail', 'blizzard', 'thunderstorm', 'hurricane'])]
event_dates_country_name = events_data[['event_date', 'country', 'event_name']].copy()
previous_day = event_dates_country_name[['event_date', 'country', 'event_name']].copy()
previous_day['event_date'] = previous_day['event_date'] - pd.to_timedelta(1, unit='d') # Add clock date and set to day before event date
next_day = event_dates_country_name[['event_date', 'country', 'event_name']].copy()
next_day['event_date'] = next_day['event_date'] + pd.to_timedelta(1, unit='d') # Add clock date and set to day after event date
event_dates = pd.concat([event_dates_country_name, previous_day, next_day], ignore_index=True).drop_duplicates().reset_index(drop=True)

print("\nSampling your results and checking that each employee attended 3 or more events and that the events listed for each employee are correct...\n")
# Randomly choose 5 rows from results data
results_sample = results_data.sample(5)
break_outer_loop = False

# For each employee in the results sample...
for row in results_sample.itertuples():
    empid = row.record_id
    empname = row.name
    empcountry = row.country

    employee_events = pd.DataFrame(row.events) # Extract the event data from the events column
    employee_events['event_date'] = pd.to_datetime(employee_events['event_date'], format='%Y-%m-%d') # Convert 'event_date' column to datetime object

    bad_wthr = bad_weather[bad_weather['country'] == empcountry].copy() # Filter the extreme weather for the employee's country
    bad_wthr['date'] = pd.to_datetime(bad_wthr['date'], format='%Y-%m-%d') # Convert 'event_date' column to datetime object

    if len(employee_events) <=3: # Check that the employee attended more than 3 events
        print(f"{empname}/{empid} did not attend more than 3 events! Please recheck your logic!")
        break
    else:
        # For each event the employee has attended...
        for row in employee_events.itertuples():
            evt_date = pd.to_datetime(row.event_date, format='%Y-%m-%d') # Convert 'event_date' column to datetime object
            evt_name = row.event_name

            # Filter event dates for the employee's country and the event name and for the day before, after and of the event
            evt_dates = event_dates[(event_dates['country'] == empcountry) & (event_dates['event_name'] == evt_name)]
            evt_dates = evt_dates[evt_dates['event_date'].between(evt_date - pd.to_timedelta(1, unit='d'), evt_date + pd.to_timedelta(1, unit='d'))]

            # Filter attendance data for the employee's late clock ins, early clock outs and absences
            employee_attendance = attendance_data[attendance_data['employee_record_id'] == empid]
            employee_attendance = employee_attendance[
                (employee_attendance['clock_in'].dt.time > pd.to_datetime('08:15:00', format='%H:%M:%S').time()) |
                (employee_attendance['clock_out'].dt.time < pd.to_datetime('16:00:00', format='%H:%M:%S').time()) |
                (employee_attendance['clock_in'].isnull() & employee_attendance['clock_out'].isnull())]

            # Filter out days with extreme weather
            employee_attendance = employee_attendance[~employee_attendance['date'].isin(bad_wthr['date'])]

            # Filter for the day before, after and of the event <-- This gives us the rows in attendance data for the employee corresponding to the event (with problematic clocks or absences and good weather)
            employee_attendance = employee_attendance[employee_attendance['date'].isin(evt_dates['event_date'])]

            # Check and report if the employee attended the event
            if employee_attendance.empty:
                print (f"{empname}/{empid} did not attend {evt_name}! Please recheck your logic!")
                break_outer_loop = True
                break # Stop the loop if an anomaly is found
            else:
                print (f"{empname}/{empid} attended {evt_name}")

    if break_outer_loop:
        break

# Report success if no anomalies are found
if not break_outer_loop:
    print("\nThe 5 employees tested were correctly listed in your results!\n")

print("\nSampling employees not listed in your results and checking that each employee did not attend 3 or more events...\n")

# Randomly choose 5 rows from employee data who do not appear in your results
good_employees = employee_data[~employee_data['record_id'].isin(results_data['record_id'])]
goodemp_sample = good_employees.sample(5)
no_issues = True

# For each employee in the sample...
for row in goodemp_sample.itertuples():
    empid = row.record_id
    empname = row.name
    empcountry = row.country

    bad_wthr = bad_weather[bad_weather['country'] == empcountry] # Filter the extreme weather for the employee's country
    evt_dates = event_dates[event_dates['country'] == empcountry] # Filter event dates for the employee's country
    employee_attendance = attendance_data[attendance_data['employee_record_id'] == empid] # Filter attendance data for the employee

    # Filter attendance data for the employee's late clock ins, early clock outs and absences
    employee_attendance = employee_attendance[
        (employee_attendance['clock_in'].dt.time > pd.to_datetime('08:15:00', format='%H:%M:%S').time()) |
        (employee_attendance['clock_out'].dt.time < pd.to_datetime('16:00:00', format='%H:%M:%S').time()) |
        (employee_attendance['clock_in'].isnull() & employee_attendance['clock_out'].isnull())]

    # Filter out days with extreme weather
    employee_attendance = employee_attendance[~employee_attendance['date'].isin(bad_wthr['date'])]

    # Filter for the day before, after and of the event <-- This gives us the rows in attendance data for the employee corresponding to an event (with problematic clocks or absences and good weather)
    employee_attendance = employee_attendance[employee_attendance['date'].isin(evt_dates['event_date'])]

    # If more than 3 problematic clocks or absences are found, report the anomaly and break the loop. Note that there may be multiple rows corresponding to the same event. Use the drilldown csv file to check the number of events.
    if len(employee_attendance) > 3:
        print(f"{empname}/{empid} had more than 3 problematic clocks or absences. Please check that these were not more than 3 separate events.")
        no_issues = False
        break
    else:
        print(f"Tested {empname}/{empid}: {len(employee_attendance)} events.")

# Report success if no anomalies are found
if no_issues:
    print("\nThe 5 employees tested were correctly not listed in your results!\n")