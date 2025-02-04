import pandas as pd
import util
import time

start_time, end_time = util.parse_args()
start_time = str(start_time)
end_time = str(end_time)


t1_start_time = time.perf_counter_ns()
df = pd.read_csv("../2022_place_canvas_history.csv", engine="pyarrow")

df =  df.loc[(df['timestamp'] > start_time) & (df['timestamp'] <= end_time)]
df = df.groupby(['pixel_color', 'coordinate']).agg([
    pd.count().alias("pixel_count")
])
df = df.sort("pixel_count", descending=True).limit(1)

# Filter to only include timestamps within given args
t1_end_time = round(int(time.perf_counter_ns() - t1_start_time) / 1000000)

util.print_result(df['pixel_color'][0], df['coordinate'][0], start_time, end_time, t1_end_time)
