import datetime
import sys

def split_time(time, hr):
    split_time = time.split('-')
    return datetime.datetime(int(split_time[0]), int(split_time[1]), int(split_time[2]), int(hr))

def parse_args():
    args = sys.argv[1:]
    if (len(args) != 4):
        print("Invalid arguments")
        sys.exit()

    start_time = split_time(args[0], args[1])
    end_time = split_time(args[2], args[3])

    if (start_time > end_time):
        print("Invalid time")
        sys.exit()
    return (start_time, end_time)

def print_result(color, location, start_time, end_time, compute):
    print(f"- **Timeframe:** {start_time} {end_time}")
    print(f"- **Execution Time:** {compute} ms")
    print(f"- **Most Placed Color:** {color}")
    print(f"- **Most Placed Pixel Location:** {location}")