with open("../2022_place_canvas_history.csv", "r") as f:
    lines = [line.replace(" UTC", "") for line in f]

with open("cleaned_canvas_history.csv", "w") as f:
    f.writelines(lines)