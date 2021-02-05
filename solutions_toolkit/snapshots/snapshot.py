import json
import pandas as pd
import numpy as np
from typing import Iterable

import glob
import os

from solutions_toolkit.snapshots.utils import get_submissions, get_submission_labels, find_overlaps

TARGET_COL = "target"
LABEL_COL = "question"
FILE_NAME_COL = "file_name"
ROW_INDEX_COL = "row_index"
COMPLETE_STATUS = "COMPLETE"


class Snapshot:
    def __init__(
        self,
        snapshot_df,
        label_col=None,
        row_index_col=None,
        text_col=None,
        filename_col=None,
        filename=None,
    ):

        self.filename = filename

        if label_col:
            self.label_col = label_col
        else:
            self.label_col = self._get_col(LABEL_COL, snapshot_df)

        if row_index_col:
            self.row_index_col = row_index_col
        else:
            self.row_index_col = self._get_col("row_index", snapshot_df)

        if text_col:
            self.text_col = text_col
        else:
            self.text_col = "text"

        if filename_col:
            self.filename_col = filename_col
        else:
            self.filename_col = self._get_col("file_name", snapshot_df)

        self.index_cols = [self.row_index_col, self.filename_col]

        unique_snapshot_df = snapshot_df.drop_duplicates(subset=self.row_index_col)

        label_cols = self.index_cols + [self.label_col]
        self.label_df = unique_snapshot_df[label_cols].set_index(self.index_cols)

        text_cols = self.index_cols + [self.text_col]
        self.text_df = unique_snapshot_df[text_cols].set_index(self.index_cols)

    def remove_classes(self, classes_to_remove: Iterable[str]) -> None:

        for i, row in self.label_df.iterrows():
            current_label_list = json.loads(row[self.label_col])
            removed_label_list = []

            for label in current_label_list:
                if label["label"] not in classes_to_remove:
                    removed_label_list.append(label)
            removed_label_string = json.dumps(removed_label_list)
            self.label_df.loc[i, self.label_col] = removed_label_string

    def get_label_list(self) -> Iterable[str]:
        label_set = set()
        for i, row in self.label_df.iterrows():
            labels = json.loads(row[self.label_col])
            for label in labels:
                label_set.add(label["label"])
        return list(label_set)

    def replace_label_name(self, original_label_name, new_label_name):
        for i, row in self.label_df.iterrows():
            labels = json.loads(row[self.label_col])
            for label in labels:
                if label["label"] == original_label_name:
                    label["label"] = new_label_name
            self.label_df.loc[i, self.label_col] = json.dumps(labels)

    def to_df(self):
        snapshot_df = pd.concat([self.label_df, self.text_df], axis=1).reset_index()
        return snapshot_df

    def to_csv(self, output_path):
        snapshot_df = self.to_df()
        snapshot_df.to_csv(output_path, index=False)

    @classmethod
    def from_csv(cls, csv_filepath, **kwargs):
        snapshot_df = pd.read_csv(csv_filepath)
        return cls(snapshot_df, **kwargs)

    @classmethod
    def from_review_queue(
        cls, client, dataset_export_df, model_name, workflow_id, label_col=LABEL_COL, **kwargs
    ):
        """
        Generate a snapshot from all documents in the review queue

        Key assumptions:
        -all complete submissions in review queue are labeled as if they were
         being used in a teach task.

        -all documents must be uploaded to the dataset so they have a row index

        Questions for later: should I just input submission ids here?
                             how can this be sped up for concurrency?
        """

        complete_submissions = get_submissions(
            client, workflow_id, COMPLETE_STATUS, False
        )

        # create label dataframe
        submission_label_rows = []
        for submission in complete_submissions:

            labels = get_submission_labels(client, submission, model_name)
            # remove unecessaary keys from submission
            [label.pop(key, None) for key in ["text", "confidence"] for label in labels]
            labels_string = json.dumps(labels)
            row = {FILE_NAME_COL: submission.input_filename, label_col: labels_string}
            submission_label_rows.append(row)
        label_df = pd.DataFrame(submission_label_rows)
        label_df.drop_duplicates(subset=FILE_NAME_COL, inplace=True)
        snapshot_df = label_df.merge(dataset_export_df, on=[FILE_NAME_COL])
        return cls(snapshot_df, label_col=label_col, **kwargs)

    @staticmethod
    def _get_col(col_string, snapshot_df):
        return [c for c in snapshot_df if c.startswith(col_string)][0]

    @classmethod
    def merge_snapshots(cls, snapshots):
        """
        This can only combine snapshots from the same dataset

        Generates a csv file
        """
        label_df = _merge_labels(snapshots)
        text_df = _merge_text(snapshots, label_df)
        snapshot_df = pd.concat([label_df, text_df], axis=1).reset_index()
        text_col = snapshots[0].text_col
        return cls(snapshot_df, text_col=text_col, label_col=LABEL_COL)

    @classmethod
    def stack(cls, snapshots, label_col=TARGET_COL, new_dataset=False, **kwargs):
        """
        stack snapshots ontop of each oher and rename index values
        """
        index_cols = [ROW_INDEX_COL, FILE_NAME_COL]
        label_df = pd.DataFrame(
            np.vstack([s.label_df.reset_index().values for s in snapshots]),
            columns=[ROW_INDEX_COL, FILE_NAME_COL, label_col],
        ).set_index(index_cols)
        text_df = pd.DataFrame(
            np.vstack([s.text_df.reset_index().values for s in snapshots]),
            columns=[ROW_INDEX_COL, FILE_NAME_COL, "text"],
        ).set_index(index_cols)
        snapshot_df = pd.concat([label_df, text_df], axis=1).reset_index()
        snapshot_df = snapshot_df.drop_duplicates(subset=FILE_NAME_COL)
        # WTF why did I do this?
        if new_dataset:
            snapshot_df[ROW_INDEX_COL] = range(0, snapshot_df.shape[0])

        return cls(snapshot_df, label_col=label_col, **kwargs)


def _merge_labels(snapshots, join="inner", label_col=LABEL_COL):
    """
    Currently we just remove overlapping labels
    """
    label_dfs = [s.label_df for s in snapshots]
    labelset_cols = [s.label_col for s in snapshots]
    index_cols = snapshots[0].index_cols
    
    label_df = pd.concat(label_dfs, axis=1, join=join)
    label_df[label_col] = None

    overlapping_data = {}
    for i, row in label_df.iterrows():
        # initialize merge with first set of labels
        merged_label = json.loads(row[labelset_cols[0]])
        overlap = False

        for labelset_col in labelset_cols[1:]:

            labels_to_merge = json.loads(row[labelset_col])

            # don't include files where labels overlap
            # TODO: Simplify this
            overlaps = find_overlaps(merged_label, labels_to_merge)
            if overlaps:
                overlap = True
                print("WARNING: Overlapping labels")
                print(overlaps)
                print(row)
            else:
                merged_label = merged_label + labels_to_merge
        if not overlap:
            label_df.loc[i, label_col] = json.dumps(merged_label)
    return label_df[[label_col]].dropna()


def _merge_text(snapshots, merged_labels):
    text_dfs = [s.text_df for s in snapshots]
    merged_text = pd.concat(text_dfs)
    merged_text = merged_text.loc[merged_labels.index]
    return merged_text.drop_duplicates()


if __name__ == "__main__":
    snapshot_path = "/home/fitz/Documents/customers/cushman-wakefield/GOS/model_retrain_with_csv/data/snapshot/"
    snapshot_filename = "GOS Combined Dataset New Model 11-12-2020.csv"
    snapshot_filepath = os.path.join(snapshot_path, snapshot_filename)
    snapshot = Snapshot.from_csv(
        snapshot_filepath, label_col="labels", filename_col="filename"
    )
    snapshot.get_label_list()
