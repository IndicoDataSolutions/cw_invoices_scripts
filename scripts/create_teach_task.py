"""
Create a labeled teach task from a labeled dataset using custom graphql queries

This is a work around until the ability to add data to teach tasks is supported
in platform

The dataset used in this example is a labeled csv file for a CW use case

NOTE!!!!! This code isn't modular enough to handle labels from stacked snapshots
"""
import os
import pandas as pd
import json
from typing import Iterable
import glob

from solutions_toolkit.indico_wrapper import IndicoWrapper
from solutions_toolkit.create_teach_task_from_labeled_dataset.graphql_queries import (
    CREATE_TEACH_TASK,
    GET_TEACH_TASK,
    SUBMIT_QUESTIONNAIRE_EXAMPLE,
)


def create_teach_task(
    indico_wrapper, dataset_id, teach_task_name, classes, dataset_type="document"
):
    dataset = indico_wrapper.get_dataset(dataset_id)
    source_col_id = dataset.datacolumn_by_name(dataset_type).id
    variables = {
        "name": teach_task_name,
        "processors": [],
        "dataType": "TEXT",
        "datasetId": dataset_id,
        "numLabelersRequired": 1,
        "sourceColumnId": source_col_id,
        "questions": [
            {
                "type": "ANNOTATION",
                "targets": classes,
                "keywords": [],
                "text": teach_task_name,
            }
        ],
    }
    teach_task = indico_wrapper.graphQL_request(CREATE_TEACH_TASK, variables)
    teach_task_id = teach_task["createQuestionnaire"]["id"]
    variables = {"id": teach_task_id}
    teach_task_stats = indico_wrapper.graphQL_request(GET_TEACH_TASK, variables)
    labelset_id = teach_task_stats["questionnaires"]["questionnaires"][0]["questions"][
        0
    ]["labelset"]["id"]
    model_group_id = teach_task_stats["questionnaires"]["questionnaires"][0][
        "questions"
    ][0]["modelGroupId"]
    return labelset_id, model_group_id


def label_teach_task(
    indico_wrapper,
    dataset_id,
    labelset_id,
    model_group_id,
    label_df,
    label_col,
    row_index_col=None,
):
    if row_index_col is None:
        row_index_col = f"row_index_{dataset_id}"

    labels = []
    for _, row in label_df.iterrows():
        row_index = row[row_index_col]
        target = row[label_col]
        labels.append({"rowIndex": row_index, "target": target})

    variables = {
        "datasetId": dataset_id,
        "labelsetId": labelset_id,
        "labels": labels,
        "modelGroupId": model_group_id,
    }

    submit = indico_wrapper.graphQL_request(SUBMIT_QUESTIONNAIRE_EXAMPLE, variables)


def get_label_list(labeled_data_df, label_col) -> Iterable[str]:
    label_set = set()
    for i, row in labeled_data_df.iterrows():
        labels = json.loads(row[label_col])
        for label in labels:
            label_set.add(label["label"])
    return list(label_set)


def main(indico_wrapper, config, teach_task_name, snapshot_filepath):
    # NOTE: Configure
    DATASET_ID = config["dataset_id"]

    # NOTE: CONFIGURE
    LABEL_COL = config["label_col"]

    row_index_col = config["row_index_col"]
    # Get labeled dataset
    labeled_data_df = pd.read_csv(snapshot_filepath)

    # NOTE: Configure - Dummy classes allow for the ability to have label tags in review
    dummy_classes = []

    # NOTE: these are classes that have never been labeled in the teach task
    # TODO: there should be an automatic way to determine this (compare to old data?)
    empty_classes = []

    # NOTE: Configure - label names of all labels in the indico produced model
    model_classes = get_label_list(labeled_data_df, LABEL_COL)
    full_classes = dummy_classes + model_classes + empty_classes

    labelset_id, model_group_id = create_teach_task(
        indico_wrapper, DATASET_ID, teach_task_name, full_classes, dataset_tye="text"
    )

    label_teach_task(
        indico_wrapper,
        DATASET_ID,
        labelset_id,
        model_group_id,
        labeled_data_df,
        LABEL_COL,
        row_index_col=row_index_col,
    )


if __name__ == "__main__":
    HOST = os.getenv("INDICO_API_HOST", "cush.indico.domains")
    API_TOKEN_PATH = "../../indico_api_token.txt"

    indico_wrapper = IndicoWrapper(HOST, API_TOKEN_PATH)

    config = {
        "dataset_id": 49,
        "label_col": "labels",
        "row_indox_col": "row_index_49",
    }

    snapshot_dir = "/path/to/snapshots"
    filename_regex = "coupon_common_*.csv"
    snapshot_filepaths = glob.glob(os.path.join(snapshot_dir, filename_regex))
    
    for i, snapshot_filepath in enumerate(snapshot_filepaths):
        teach_task_name = f"Coupon_Common Split model: {i}"
        main(indico_wrapper, config, teach_task_name, snapshot_filepath)
