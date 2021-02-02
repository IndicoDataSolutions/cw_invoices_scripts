import pandas as pd

original_snapshot_filepath = "/home/fitz/Documents/customers/cushman-wakefield/COI/coi_full_labels.csv"
retraining_csv_filepath = "/home/fitz/Documents/customers/cushman-wakefield/COI/coi_retraining_labels.csv"

df1 = pd.read_csv(original_snapshot_filepath)
df2 = pd.read_csv(retraining_csv_filepath)

full_df = pd.concat([df1, df2])
full_df[["filename", "text", "labels"]].to_csv("coi_merged_data.csv", index=False)
