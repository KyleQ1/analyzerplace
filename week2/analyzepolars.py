import polars as pl
import util
import time
import util

start_time, end_time = util.parse_args()
start_time = str(start_time)
end_time = str(end_time)

t1_start_time = time.perf_counter_ns()

df = pl.read_csv("../2022_place_canvas_history.csv")

# parse dates
df = df.with_columns(
    df['timestamp'].str.slice(0, 13).alias("hour")
)

# Filter by timestamp then group by pixel color and coordinate
df = df.filter((pl.col('timestamp') > start_time) & (pl.col('timestamp') <= end_time)) 
df = df.group_by(['pixel_color', 'coordinate']).agg([
        pl.len().alias("pixel_count")
    ])
df = df.sort("pixel_count", descending=True).limit(1)

t1_end_time = round(int(time.perf_counter_ns() - t1_start_time) / 1000000)

util.print_result(df['pixel_color'][0], df['coordinate'][0], start_time, end_time, t1_end_time)