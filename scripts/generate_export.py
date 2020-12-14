import os
import pandas as pd
from collections import defaultdict
from indico.queries import (
    RetrieveStorageObject,
    SubmissionResult,
    UpdateSubmission,
    SubmissionFilter,
    ListSubmissions,
)
from indico import IndicoClient, IndicoConfig


# NOTE, Configure
HOST = os.getenv("INDICO_API_HOST", "cush.indico.domains")

# NOTE, Configure
API_TOKEN_PATH = "../../indico_api_token.txt"
with open(API_TOKEN_PATH) as f:
    API_TOKEN = f.read()

my_config = IndicoConfig(host=HOST, api_token=API_TOKEN, verify_ssl=False)
INDICO_CLIENT = IndicoClient(config=my_config)
EXCEPTION_STATUS = "PENDING_ADMIN_REVIEW"
COMPLETE_STATUS = "COMPLETE"

# NOTE, please configure this to the appropriate ID
WORKFLOW_ID = 131

# NOTE, please configure this to the appropriate model name
MODEL_NAME = "VA Merged teach tasks q93 model"

# Field types
# There should only be one value for each key field
DOC_KEY_FIELDS = [
    "Comm Slip: Is Signature Valid?",
    "LOE: Is Signature Valid?",
    "Comm Slip: Does Document Exist?",
    "LOE: Does Document Exist?",
    "POD: Does Document Exist?",
    "Invoice: Does Document Exist?",
    "Comm Slip: Gross Fee Total",
    "LOE:  Total Fee",
]

# There should only be one value on each page
PAGE_KEY_FIELDS = []

# Row fields will be aggregated into rows
ROW_FIELDS = [
    "Comm Slip: No Prof Part Allocations",
    "Comm Slip: Name - Prof Part",
    "Comm Slip: Allocation - Prof Part",
    "Comm Slip: Name - Outside Prof Part",
    "Comm Slip: Earnings - Outside Prof Part",
    "Comm Slip: Name - CW Brokers Part",
    "Comm Slip: Earnings - CW Brokers Part",
    "Comm Slip: Name - Outside CW Part",
    "Comm Slip: Earnings - Outside CW Part",
]

EXPORT_DIR = "./"


def assign_confidences(results, model_name):
    """
    Append confidences to predictions
    """
    preds_pre_review = results["results"]["document"]["results"][model_name][
        "pre_review"
    ]
    preds_final = results["results"]["document"]["results"][model_name]["final"]

    for pred_final in preds_final:
        final_start = pred_final["start"]
        final_end = pred_final["end"]
        for pred_pre_review in preds_pre_review:
            pre_start = pred_pre_review["start"]
            pre_end = pred_pre_review["end"]
            if final_start == pre_start and final_end == pre_end:
                pred_final["confidence"] = pred_pre_review["confidence"]
    return preds_final


def get_page_extractions(submission, model_name, post_review=True):
    """
    Return predictions and page info for a submission

    post_review is a flag to select either the final reviewed values
    or the pre reviewed values
    """
    if post_review:
        result_type = "final"
    else:
        result_type = "pre_review"

    sub_job = INDICO_CLIENT.call(SubmissionResult(submission.id, wait=True))
    results = INDICO_CLIENT.call(RetrieveStorageObject(sub_job.result))

    # get page info
    etl_output_url = results["etl_output"]
    etl_output = INDICO_CLIENT.call(RetrieveStorageObject(etl_output_url))

    page_infos = []
    for page in etl_output["pages"]:
        page_info_url = page["page_info"]
        page_text_dict = INDICO_CLIENT.call(RetrieveStorageObject(page_info_url))
        page_infos.append(page_text_dict)

    # get predictions
    if result_type == "pre_review":
        predictions = results["results"]["document"]["results"][model_name][result_type]
    else:
        # doc was rejected from review queue
        if results.get("review_rejected"):
            predictions = None
        else:
            predictions = assign_confidences(results, model_name)
    return page_infos, predictions


def merge_page_tokens(pages):
    """
    Return a list of tokens from a list of pages
    """
    tokens = []
    for page in pages:
        tokens.extend(page["tokens"])
    return tokens


def filter_preds(predictions, label_set):
    return [pred for pred in predictions if pred["label"] in label_set]


def add_page_number(page_predictions, tokens):
    for pred in page_predictions:
        if pred["start"]:
            start, end = (
                pred["start"] - 1,
                pred["end"] + 1,
            )
            for token in tokens:
                if (
                    token["doc_offset"]["start"] >= start
                    and token["doc_offset"]["end"] <= end
                ):
                    pred["page_num"] = token["page_num"]
                    break
            else:
                pred["page_num"] = 999


def align_rows(row_predictions, tokens, filename):
    """
    Main logic for aligning data
    row_predictions: a list of line item predictions from 1 document
    tokens: list of token dictionaries from pdf extraction
    filename: string name of file predicted on
    """

    # Indico prediction positions are given as spans in the document string
    # these spans need to be resolved with the positioning of the tokens in the doc
    positions = list()
    page_preds = sorted(row_predictions, key=lambda x: x["start"])
    for pred in page_preds:
        start, end = (
            pred["start"] - 1,
            pred["end"] + 1,
        )
        position = dict()
        for token in tokens:
            if (
                token["doc_offset"]["start"] >= start
                and token["doc_offset"]["end"] <= end
            ):
                position["bbtop"] = token["position"]["bbTop"]
                position["bbbot"] = token["position"]["bbBot"]
                position["bbleft"] = token["position"]["bbLeft"]
                position["bbright"] = token["position"]["bbRight"]
                position["page_num"] = pred["page_num"]
                position["label"] = pred["label"]
                position["text"] = pred["text"]
                # handle the case where labels are adjusted in review and contain no confidence

                if pred.get("confidence"):
                    confidence = pred["confidence"].get(position["label"])
                    if confidence:
                        position["confidence"] = confidence
                    else:
                        position["confidence"] = 1.0
                else:
                    position["confidence"] = 1.0
                break
        if not position:
            continue
        positions.append(position)

    new_list = []
    if positions:
        # we now group these values by their y position in the page

        new_positions = sorted(
            positions, key=lambda x: (x["page_num"], x["bbtop"], x["bbleft"])
        )
        max_top = new_positions[0]["bbtop"]
        min_bot = new_positions[0]["bbbot"]
        current_page = new_positions[0]["page_num"]
        row = defaultdict(list)
        for i in new_positions:
            if i["bbtop"] > min_bot or i["page_num"] > current_page:
                new_list.append(row)
                row = defaultdict(list)
                max_top, min_bot, current_page = i["bbtop"], i["bbbot"], i["page_num"]
                row[i["label"]].append(i)
            else:
                row[i["label"]].append(i)
                max_top, min_bot = max(i["bbtop"], max_top), min(i["bbbot"], min_bot)
        new_list.append(row)

    # convert to a data frame
    line_item_df = aligned_rows_to_df(new_list)
    line_item_df["filename"] = filename
    return line_item_df


def predictions_to_df(submissions, doc_predictions, page_num=False):
    """
    Convert list of prediction dicts to a vertical df
    with columns [filename, label, value, confidence]
    """
    pred_dict = defaultdict(list)
    for sub, doc_prediction in zip(submissions, doc_predictions):
        for prediction in doc_prediction:
            label = prediction["label"]
            pred_dict["label"].append(label)
            pred_dict["text"].append(prediction["text"])
            if page_num:
                pred_dict["page_num"].append(prediction["page_num"])
            if "confidence" in prediction:
                pred_dict["confidence"].append(prediction["confidence"][label])
            else:
                pred_dict["confidence"].append(1.0)
            pred_dict["filename"] = sub.input_filename

    pred_df = pd.DataFrame(pred_dict)
    return pred_df


def get_top_pred_df(pred_df, page_num=False):

    if page_num:
        groupby_cols = ["filename", "page_num", "label"]
    else:
        groupby_cols = ["filename", "label"]

    pred_highest_df = pred_df.loc[pred_df.groupby(groupby_cols)["confidence"].idxmax()]
    return pred_highest_df


def vert_to_horizontal(top_conf_df, page_num=False):
    """
    Assumes that df only contains one value per label
    """
    if page_num:
        cols = ["filename", "page_num", "label"]
    else:
        cols = ["filename", "label"]

    pivot_val_cols = ["text", "confidence"]
    dfs = []
    for pivot_val_col in pivot_val_cols:
        columns = cols + [pivot_val_col]
        subset_df = top_conf_df[columns].set_index(cols)
        pred_comparison_df = subset_df.unstack()[pivot_val_col].add_suffix(
            f" {pivot_val_col}"
        )
        dfs.append(pred_comparison_df)

    pred_wide_df = pd.concat(dfs, axis=1)

    # reorder columns
    labels = pd.unique(top_conf_df["label"]).tolist()
    col_order = []
    for label in labels:
        for pivot_val_col in pivot_val_cols:
            col_order.append(f"{label} {pivot_val_col}")
    return pred_wide_df[col_order].reset_index()


def aligned_rows_to_df(aligned_rows):
    df_rows = []
    for aligned_row in aligned_rows:
        df_row = defaultdict(str)
        max_count = 0
        for label, values in aligned_row.items():
            if len(values) > max_count:
                max_count = len(values)
        rows = [defaultdict(str) for i in range(max_count)]

        for label, values in aligned_row.items():
            # concatenate multiple values
            # TODO: need to investigate whether this is a correct approach
            for col, value in enumerate(values):
                rows[col][f"{label} text"] = str(value["text"])
                rows[col][f"{label} confidence"] = value["confidence"]
                rows[col]["page_num"] = value["page_num"]

        for row in rows:
            df_rows.append(row)

    return pd.DataFrame(df_rows)


def get_submissions(client, workflow_id, status, retrieved):
    sub_filter = SubmissionFilter(status=status, retrieved=retrieved)
    complete_submissions = client.call(
        ListSubmissions(workflow_ids=[workflow_id], filters=sub_filter)
    )
    return complete_submissions


def mark_retreived(client, submission_id):
    client.call(UpdateSubmission(submission_id, retrieved=True))


def contains_added_text(predictions, fields):
    for pred in predictions:
        if (
            (pred["start"] == 0 and pred["end"] == 0)
            or (pred["start"] is None and pred["end"] is None)
        ) and pred["label"] in fields:
            return True
    return False


if __name__ == "__main__":
    retrieved = False
    exception_ids = []
    exception_filenames = []

    # To export COMPLETE submissions
    complete_submissions = get_submissions(
        INDICO_CLIENT, WORKFLOW_ID, COMPLETE_STATUS, retrieved
    )

    # FULL WORK FLOW
    full_dfs = []
    for submission in complete_submissions:
        page_infos, predictions = get_page_extractions(
            submission, MODEL_NAME, post_review=True
        )
        if predictions:

            # first check for manually added preds, add them to exception queue
            if contains_added_text(predictions, ROW_FIELDS + PAGE_KEY_FIELDS):
                exception_ids.append(int(submission.id))
                exception_filenames.append(str(submission.input_filename))
                # mark_retreived(INDICO_CLIENT, submission.id)
                continue

            tokens = merge_page_tokens(page_infos)
            add_page_number(predictions, tokens)
            doc_key_predictions = filter_preds(predictions, DOC_KEY_FIELDS)
            page_key_predictions = filter_preds(predictions, PAGE_KEY_FIELDS)
            row_predictions = filter_preds(predictions, ROW_FIELDS)

            # this may need it's own function/ better abstraction
            if doc_key_predictions:
                key_preds_vert_df = predictions_to_df(
                    [submission], [doc_key_predictions]
                )
                top_conf_key_pred_df = get_top_pred_df(key_preds_vert_df)
                key_pred_df = vert_to_horizontal(top_conf_key_pred_df)

            if page_key_predictions:
                key_preds_vert_df = predictions_to_df(
                    [submission], [page_key_predictions], page_num=True
                )
                top_conf_key_pred_df = get_top_pred_df(key_preds_vert_df, page_num=True)
                page_key_pred_df = vert_to_horizontal(
                    top_conf_key_pred_df, page_num=True
                )

            if row_predictions:
                line_item_df = align_rows(
                    row_predictions, tokens, submission.input_filename
                )

            # TODO: this logic is gross
            if doc_key_predictions:
                if row_predictions and page_key_predictions:
                    full_df = key_pred_df.merge(page_key_pred_df, how="outer")
                    full_df = full_df.merge(
                        line_item_df, on=["filename", "page_num"], how="outer"
                    )
                else:
                    if row_predictions:
                        full_df = key_pred_df.merge(line_item_df, how="outer")
                    elif page_key_predictions:
                        full_df = key_pred_df.merge(page_key_pred_df)
                    else:
                        full_df = key_pred_df

            elif row_predictions:
                if page_key_predictions:
                    full_df = line_item_df.merge(
                        page_key_pred_df, on=["filename", "page_num"], how="outer"
                    )
                else:
                    full_df = line_item_df

            elif doc_key_predictions:
                full_df = page_key_pred_df

            else:
                continue
            full_dfs.append(full_df)

    if full_dfs:
        output_df = pd.concat(full_dfs)
        labels = DOC_KEY_FIELDS + PAGE_KEY_FIELDS + ROW_FIELDS
        col_order = ["filename"]
        pivot_val_cols = ["text", "confidence"]
        for label in labels:
            for pivot_val_col in pivot_val_cols:
                col_order.append(f"{label} {pivot_val_col}")

        current_cols = set(output_df.columns)
        missing_cols = list(set(col_order).difference(current_cols))
        for missing_col in missing_cols:
            output_df[missing_col] = None

        doc_key_text_cols = [f"{col} text" for col in DOC_KEY_FIELDS]
        doc_key_conf_cols = [f"{col} confidence" for col in DOC_KEY_FIELDS]
        output_df.reset_index(drop=True, inplace=True)
        output_df[doc_key_text_cols] = output_df.groupby(["filename"], sort=False)[
            doc_key_text_cols
        ].apply(lambda x: x.ffill().bfill())
        output_df[doc_key_conf_cols] = output_df.groupby(["filename"], sort=False)[
            doc_key_conf_cols
        ].apply(lambda x: x.ffill().bfill())
        output_df = output_df[col_order]

        output_filepath = os.path.join(EXPORT_DIR, "VA_export.csv")
        output_df.to_csv(output_filepath, index=False)

        print("An export file has been generated")

        # for sub in complete_submissions:
        # mark_retreived(INDICO_CLIENT, sub.id)

    else:
        print("No COMPLETE submissions to generate export")

    EXCEPTION_STATUS = "PENDING_ADMIN_REVIEW"
    exception_submissions = get_submissions(
        INDICO_CLIENT, WORKFLOW_ID, EXCEPTION_STATUS, retrieved
    )

    # Creating a DataFrame to store Exception Submission IDs and their corresponding filenames

    for es in exception_submissions:
        exception_ids.append(int(es.id))

    exception_ids_df = pd.DataFrame(exception_ids, columns=["Submission ID"])

    for es in exception_submissions:
        exception_filenames.append(str(es.input_filename))

    exception_filenames_df = pd.DataFrame(exception_filenames, columns=["File Name"])
    exceptions_df = pd.concat([exception_ids_df, exception_filenames_df], axis=1)

    # for sub in exception_submissions:
    # mark_retreived(INDICO_CLIENT, sub.id)

    # Exporting Exception files and their Submission IDs as a CSV
    exception_filepath = os.path.join(EXPORT_DIR, "VA_exceptions.csv")
    exceptions_df.to_csv(exception_filepath, index=False)

    print("An exception file has been generated")