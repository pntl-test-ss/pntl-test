# Employee Behavior Analysis

## Overview

This repository contains scripts for analyzing data from employee, attendance, weather, and event JSON files. The goal is to identify employees exhibiting patterns of tardiness, leaving early, or absenteeism on the day of, day after, or day before an event in their respective countries, with exceptions for days with extreme weather conditions. 

## Files and Directories

- employees.json: JSON file containing employee information.
- attendance.json: JSON file with details about employee attendance.

## Scripts

1. identify_employees.py: Main script for identifying employees and producing a solution file, results.json
2. test_results.py: Script for testing and validating the results obtained from the analysis
3. analyze_data.py: Script used for understanding the structure, contents, and characteristics of the data within JSON files

## Dependencies

The scripts use the following dependencies:
1. Python 3.x
2. pandas
3. NumPy

Ensure these dependencies are installed before running the scripts.
   
## Usage

1. Ensure you have the required JSON files (employee_data.json, attendance_data.json).
2. Run the identify_employees.py script to perform the analysis and identify patterns of employee behavior.

   ```bash
   python identify_employees.py.py
