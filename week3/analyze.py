import polars as pl
import numpy as np
import time
import datetime
import util

def rank_colors_by_users(df):
    # Group by color and count distinct user_ids.
    ranking = (
        df.group_by("pixel_color")
        .agg(pl.col("user_id").n_unique().alias("distinct_users"))
        .sort("distinct_users", descending=True)
    )

    return ranking

def compute_average_session_length(df):    
    # For each user, we collect all timestamps in the timeframe.
    # Only users with more than one placement are needed.
    user_sessions = (
    df.group_by("user_id")
      .agg([
          pl.col("timestamp").sort().alias("timestamps"),
          pl.col("timestamp").n_unique().alias("unique_count")
      ])
    ).filter(pl.col("unique_count") > 1).drop("unique_count")

    session_lengths = []  # store session durations in seconds

    gap_threshold = datetime.timedelta(minutes=15)

    # Process each user’s timestamp list.
    for row in user_sessions.iter_rows(named=True):
        timestamps = row["timestamps"]
        if len(timestamps) < 2:
            continue
        session_start = timestamps[0]
        session_end = timestamps[0]
        user_session_lengths = []
        for ts in timestamps[1:]:
            # If gap greater than 15 minutes, end the current session.
            if ts - session_end > gap_threshold:
                duration = (session_end - session_start).total_seconds()
                if duration > 0:
                    user_session_lengths.append(duration)
                # Start a new session.
                session_start = ts
            session_end = ts
        # Add the last session if it has more than one event.
        duration = (session_end - session_start).total_seconds()
        if duration > 0:
            user_session_lengths.append(duration)
        session_lengths.extend(user_session_lengths)

    avg_session_length = round(np.mean(session_lengths), 1)

    return avg_session_length

def pixel_placement_percentiles(df):
    # Count number of placements per user.
    placements = (
        df.group_by("user_id")
        .agg(pl.len().alias("pixel_count"))
        .select("pixel_count")
        .to_series()
        .to_list()
    )

    percentiles = {
        "50th": np.percentile(placements, 50),
        "75th": np.percentile(placements, 75),
        "90th": np.percentile(placements, 90),
        "99th": np.percentile(placements, 99),
    }

    return percentiles

def count_first_time_users(df, start_dt, end_dt):
    # For each user, find the earliest placement.
    first_placements = (
        df.group_by("user_id")
        .agg(pl.col("timestamp").min().alias("first_timestamp"))
    )
    # Count the users whose first pixel is within [start_dt, end_dt).
    count = first_placements.filter(
        (pl.col("first_timestamp") >= start_dt) & (pl.col("first_timestamp") < end_dt)
    ).height

    return count


def main():
    start_dt, end_dt = util.parse_args()

    t1 = time.perf_counter_ns()
    df = pl.read_parquet("rplace.parquet")
    #print(df.head(10))

    time_df = df.filter(
        (pl.col("timestamp") >= start_dt) & (pl.col("timestamp") < end_dt)
    ).sort("timestamp")

    # Task 1
    ranking = rank_colors_by_users(time_df)
    print("Ranking of Colors")
    for i, row in enumerate(ranking.iter_rows(named=True), start=1):
        color=row["pixel_color"]
        users = row["distinct_users"]
        print(f"{i}. {color}: {users} users")

    # Task 2
    avg_session_length = compute_average_session_length(time_df)
    print("Average Session Length (seconds)")
    print(f"{avg_session_length} seconds")

    # Task 3
    percentiles = pixel_placement_percentiles(time_df)
    print("Pixel Placement Percentiles")
    for perc, val in percentiles.items():
        print(f"{perc}: {val}")

    # Task 4
    first_time_count = count_first_time_users(df, start_dt, end_dt)
    
    print("Count of First-Time Users")
    print(f"{first_time_count} users")

    # time
    time_end = round((time.perf_counter_ns() - t1) / 1000000, 1)
    print(f"{time_end} ms")

main()