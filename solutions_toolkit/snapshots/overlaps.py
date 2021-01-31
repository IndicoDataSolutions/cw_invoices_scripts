from utils import find_overlaps
import pandas as pd
import json

filename = "/home/fitz/Documents/customers/cushman-wakefield/cams/rohan_scripting/cams_teach1.csv"
csv = pd.read_csv(filename)
label_col = "question_59"

for i, row in csv.iterrows():
    labels = json.loads(row[label_col])
    overlaps = find_overlaps(labels, labels)
    if overlaps:
        print(f"{row['row_index_50']}, {row['file_name_50']}")
    