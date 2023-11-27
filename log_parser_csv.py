import re
import csv

# Define a regular expression pattern to extract information from the log file
pattern = r"(\d+:\d+:\d+\.\d+) \[.*\]: (\d+) of \d+ (PASS|FAIL) (.*?) \[.* in (\d+\.\d+s|\d+\.\d+m)]"

# Open the log file for reading
log_file_path = "logs/dbt.log.1"
with open(log_file_path, "r") as log_file:
    log_data = log_file.read()

# Find and extract information from the log using the regular expression
matches = re.findall(pattern, log_data)

# Create a CSV file to write the extracted log data
csv_file_path = "test_log.csv"
with open(csv_file_path, "w", newline="") as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(["Time", "Test Number", "Test Result", "Test Name", "Time Taken (seconds)"])

    row_count = 0  # Initialize a counter for the rows

    for match in matches:
        time, test_number, test_result, test_name, time_taken = match
        # Convert time taken to seconds if it's in minutes
        if time_taken.endswith("m"):
            time_taken = float(time_taken.rstrip("m")) * 60
        else:
            time_taken = float(time_taken.rstrip("s"))

        # Write the extracted information to the CSV file
        csvwriter.writerow([time, test_number, test_result, test_name, time_taken])
        row_count += 1  # Increment the row count

print(f"{row_count} rows of data have been written to '{csv_file_path}'.")
