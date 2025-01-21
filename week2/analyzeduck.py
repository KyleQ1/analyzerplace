import duckdb
import util
import time

start_time, end_time = util.parse_args()

t1_start_time = time.perf_counter_ns()
readcsv = duckdb.read_csv("../2022_place_canvas_history.csv", sep=",", header=True)

duckdb.sql("""
    CREATE TABLE rplace AS 
    SELECT timestamp, pixel_color, coordinate 
    FROM readcsv
""")

result = duckdb.execute("""
    SELECT pixel_color, coordinate, COUNT(*) 
    FROM rplace
    WHERE timestamp >= ? AND timestamp <= ? 
    GROUP BY pixel_color, coordinate 
    ORDER BY COUNT(*) 
    DESC LIMIT 1
""", (start_time, end_time))
t1_end_time = round(int(time.perf_counter_ns() - t1_start_time) / 1000000)

results = result.fetchone()
util.print_result(results[0], results[1], start_time, end_time, t1_end_time)
