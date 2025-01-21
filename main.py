import csv
import datetime
import sys
import time

def split_time(time, hr):
    split_time = time.split('-')
    return datetime.datetime(int(split_time[0]), int(split_time[1]), int(split_time[2]), int(hr))

def compute_csv(filename, start_time, end_time):
    color_counts = {}
    location_counts = {}

    with open(filename, "r") as csvfile:
        datareader = csv.reader(csvfile)
        # skip header
        next(datareader) 

        for row in datareader:
            timestamp_str = row[0][:-4]
            color_hex = row[2]
            coordinate = row[3]

            #print(timestamp_str, color_hex, coordinate)
            try:
                row_dt = datetime.datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue

            if row_dt < start_time:
                continue
            if row_dt > end_time:
                break

            if color_hex not in color_counts:
                color_counts[color_hex] = 0
            color_counts[color_hex] += 1

            #location = coordinate[1:-1].split(',')
            #location = (int(location[0]), int(location[1]))
           # print(location)
            if coordinate not in location_counts:
                location_counts[coordinate] = 0
            location_counts[coordinate] += 1

        most_common_color = max(color_counts, key=color_counts.get)
        most_common_location = max(location_counts, key=location_counts.get)
        
    return (most_common_color, most_common_location)
    
args = sys.argv[1:]
if (len(args) != 4):
    print("Invalid arguments")
    sys.exit()

start_time = split_time(args[0], args[1])
end_time = split_time(args[2], args[3])

if (start_time > end_time):
    print("Invalid time")
    sys.exit()

t1_start_time = time.perf_counter_ns()
color, place = compute_csv('2022_place_canvas_history.csv', start_time, end_time)
t1_end_time = round(int(time.perf_counter_ns() - t1_start_time) / 1000000)

print(f'Timeframe: {start_time} to {end_time}')
print(f'Total time it took to run in ms: {t1_end_time}')
print(f'Most place color: {color}')
print(f'Most place location: {place}')
