import os
import sys
from tqdm import tqdm
from time import sleep
import pandas as pd
from collections import defaultdict
from solutions_toolkit.auto_review import Reviewer, FieldConfiguration
from solutions_toolkit.uipath_block_scripts.config import ExportConfiguration
from solutions_toolkit.indico_wrapper import IndicoWrapper
import datetime
import logging

USAGE_STRING = "USAGE: python3 generate_export path/to/configuration_file"


def assign_confidences(results, model_name):
    """
    Append confidences to predictions
    """
    preds_pre_review = results["results"]["document"]["results"][model_name][
        "pre_review"
    ]
    preds_final = results["results"]["document"]["results"][model_name]["final"]

    for pred_final in preds_final:

        for pred_pre_review in preds_pre_review:
            if labels_equal(pred_pre_review, pred_final):
                pred_final["confidence"] = pred_pre_review["confidence"]
    return preds_final

def labels_equal(label_1, label_2):
    label_1_start = label_1["start"]
    label_2_start = label_2["start"]
    if not label_1_start == label_2_start:
        return False

    label_1_end = label_1["end"]
    label_2_end = label_2["end"]
    if not label_1_end == label_2_end:
        return False

    label_1_class = label_1["label"]
    label_2_class = label_2["label"]
    if not label_1_class == label_2_class:
        return False

    return True

def get_page_extractions(indico_wrapper, submission, model_name, post_review=False):
    """
    Return predictions and page info for a submission
    post_review is a flag to select either the final reviewed values
    or the pre reviewed values
    """
    if post_review:
        result_type = "final"
    else:
        result_type = "pre_review"

    results = indico_wrapper.get_submission_results(submission)

    # get page info
    etl_output_url = results["etl_output"]
    etl_output = indico_wrapper.get_storage_object(etl_output_url)

    page_infos = []
    for page in etl_output["pages"]:
        page_info_url = page["page_info"]
        page_text_dict = indico_wrapper.get_storage_object(page_info_url)
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

    reviewer_id = results.get("reviewer_id")
    return page_infos, predictions, reviewer_id


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
        if pred.get("start", None) is not None:
            for token in tokens:
                if sequences_overlap(token["doc_offset"], pred):
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
        position = dict()
        for token in tokens:
            if sequences_overlap(token["doc_offset"], pred):
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


def contains_added_text(predictions, fields):
    for pred in predictions:
        if (
            (pred["start"] == 0 and pred["end"] == 0)
            or (pred["start"] is None and pred["end"] is None)
        ) and pred["label"] in fields:
            return True
    return False


def sequences_overlap(x: dict, y: dict) -> bool:
    """
    Boolean return value indicates whether or not seqs overlap
    """
    return x["start"] < y["end"] and y["start"] < x["end"]


if __name__ == "__main__":
    begin_time = datetime.datetime.now()
    if len(sys.argv) != 2:
        print(USAGE_STRING)
        sys.exit()

    configuration_path = sys.argv[1]
    if not os.path.exists(configuration_path):
        print(f"configuration file: {configuration_path} does not exist")
        sys.exit()

    config = ExportConfiguration.from_yaml(configuration_path)
    FIELD_CONFIG_FILEPATH = config.field_config_filepath
    if FIELD_CONFIG_FILEPATH:
        field_config_obj = FieldConfiguration.from_yaml(FIELD_CONFIG_FILEPATH)
        field_config = field_config_obj.field_config
    HOST = config.host
    API_TOKEN_PATH = config.api_token_path

    WORKFLOW_ID = config.workflow_id
    MODEL_NAME = config.model_name

    DOC_KEY_FIELDS = config.doc_key_fields
    PAGE_KEY_FIELDS = config.page_key_fields
    ROW_FIELDS = config.row_fields
    POST_PROCESSING = config.post_processing
    BATCH_SIZE = config.export_batch_size

    EXPORT_DIR = config.export_dir
    EXPORT_FILENAME = config.export_filename
    EXCEPTION_FILENAME = config.exception_filename
    DEBUG = config.debug
    STP = config.stp
    LOG_FILE_DIR = config.log_file_dir
    LOG_FILENAME = config.log_filename
    EXCEPTION_STATUS = "PENDING_ADMIN_REVIEW"
    COMPLETE_STATUS = "COMPLETE"

    indico_wrapper = IndicoWrapper(HOST, API_TOKEN_PATH)
    retrieved = config.retrieved
    post_review = not STP
    exception_ids = []
    exception_filenames = []
    complete_ids = []
    complete_filenames = []
    sub_job = []
    result = []
    complete_revID = []
    exceptions_revID = []

    timestamp = datetime.datetime.now().strftime("%m_%d_%Y-%I_%M_%S_%p")
    logging.basicConfig(level=logging.WARNING,
                    format='%(asctime)s %(message)s',
                    datefmt='%m-%d %H:%M:%S',
                    filename=f'{LOG_FILE_DIR}\\{LOG_FILENAME}_{timestamp}.log',
                    filemode='w', force=True)
    logging.warning("Getting the list of reviewed submissions from Indico")

    # To export COMPLETE submissions
    complete_submissions = indico_wrapper.get_submissions(
        WORKFLOW_ID, COMPLETE_STATUS, retrieved
    )
    logging.warning(f"Time:{(datetime.datetime.now() - begin_time).seconds} seconds")
    total_submissions = len(complete_submissions)
    logging.warning(f"{total_submissions} submissions have been obtained")
    if BATCH_SIZE is None:
        BATCH_SIZE = total_submissions

    print(f"Starting processing of {total_submissions} submissions")
    logging.warning(f"Starting processing of {total_submissions} submissions")
    if complete_submissions:
        batched_submissions = range(0, len(complete_submissions), BATCH_SIZE)
    else:
        batched_submissions = []
        print("No COMPLETE submissions to generate export")
        logging.warning("No COMPLETE submissions to generate export")

    for batch_num, batch_start in enumerate(batched_submissions):
        batch_end = batch_start + BATCH_SIZE
        submission_batch = complete_submissions[batch_start:batch_end]
        # FULL WORK FLOW
        full_dfs = []
        print(f"Starting Batch {batch_num+1}")
        logging.warning(f"Starting Batch {batch_num+1}")
        for submission in tqdm(submission_batch):
            logging.warning(f"Time:{(datetime.datetime.now() - begin_time).seconds} seconds")
            logging.warning(f"Extracting data for {submission.input_filename} : {submission.id}")
            try:
                page_infos, predictions, reviewer_id = get_page_extractions(
                    indico_wrapper, submission, MODEL_NAME, post_review=post_review
                )
                complete_revID.append(reviewer_id)
                complete_ids.append(submission.id)
                complete_filenames.append(submission.input_filename)
                if predictions:
                    # apply post processing functions
                    if POST_PROCESSING:
                        inital_predictions = {MODEL_NAME: predictions}
                        reviewer = Reviewer(
                            inital_predictions, MODEL_NAME, field_config
                        )
                        reviewer.apply_reviews()
                        predictions = reviewer.get_updated_predictions()[MODEL_NAME]

                    # first check for manually added preds, add them to exception queue
                    if contains_added_text(predictions, ROW_FIELDS + PAGE_KEY_FIELDS):
                        logging.warning(f"for {submission.input_filename}:{submission.id}, Add Value feature was used for row field")
                        exception_ids.append(int(submission.id))
                        exception_filenames.append(str(submission.input_filename))
                        exceptions_revID.append(str(reviewer_id))
                        logging.warning(f"Marking {submission.input_filename}:{submission.id} as retrieved")
                        if not DEBUG:
                            indico_wrapper.mark_retreived(submission)
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
                        top_conf_key_pred_df = get_top_pred_df(
                            key_preds_vert_df, page_num=True
                        )
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
                                page_key_pred_df,
                                on=["filename", "page_num"],
                                how="outer",
                            )
                        else:
                            full_df = line_item_df

                    elif doc_key_predictions:
                        full_df = page_key_pred_df

                    else:
                        continue
                    full_dfs.append(full_df)
            except ConnectionError:
                exception_ids.append(int(submission.id))
                exception_filenames.append(str(submission.input_filename))
                if not DEBUG:
                    indico_wrapper.mark_retreived(submission)
                continue

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
            output_df[doc_key_text_cols] = output_df.groupby(
                ["filename"], sort=False
            )[doc_key_text_cols].apply(lambda x: x.ffill().bfill())
            output_df[doc_key_conf_cols] = output_df.groupby(
                ["filename"], sort=False
            )[doc_key_conf_cols].apply(lambda x: x.ffill().bfill())
            output_df = output_df[col_order]

            reviewer_filename_df = pd.DataFrame(
                {"Submission ID":complete_ids,"filename": complete_filenames, "Reviewer ID": complete_revID}
            )

            output_df = pd.merge(
                reviewer_filename_df, output_df, on="filename", how="outer"
            )

            output_filepath = os.path.join(EXPORT_DIR, EXPORT_FILENAME)
            output_df.to_csv(output_filepath, index=False)
            print(f"Generated export {output_filepath}")
            total_processed = min(batch_end, total_submissions)
            print(f"Processed {total_processed}/ {total_submissions}")
            print("An export file has been generated")
            logging.warning(f"Time:{(datetime.datetime.now() - begin_time).seconds} seconds")
            logging.warning("An export file has been generated")

            if not DEBUG:
                for sub in submission_batch:
                    logging.warning(f"Marking file {sub.input_filename} with Submission ID {sub.id} as retrieved")
                    indico_wrapper.mark_retreived(sub)
            logging.warning(f"Time:{(datetime.datetime.now() - begin_time).seconds} seconds") 
            logging.warning("All COMPLETE submissions have been marked retrieved")
    EXCEPTION_STATUS = "PENDING_ADMIN_REVIEW"
    logging.warning("Getting the list of rejected submissions from Indico")
    exception_submissions = indico_wrapper.get_submissions(
        WORKFLOW_ID, EXCEPTION_STATUS, retrieved
    )
    logging.warning(f"Time:{(datetime.datetime.now() - begin_time).seconds} seconds")    
    logging.warning("Rejected submissions list has been obtained")  
    # Creating a DataFrame to store Exception Submission IDs and their corresponding filenames

    logging.warning(f"Beginning to create the exceptions file with {len(exception_submissions)} submissions")
    for es in exception_submissions:
        exception_ids.append(int(es.id))
        exception_filenames.append(str(es.input_filename))
        result = indico_wrapper.get_submission_results(es)
        exceptions_revID.append(result.get("reviewer_id"))
    
    exceptions_df = pd.DataFrame(
        {
            "Submission ID": exception_ids,
            "File Name": exception_filenames,
            "Reviewer ID": exceptions_revID,
        }
    )

    if not DEBUG:
        for sub in exception_submissions:
            logging.warning(f"Marking file {sub.input_filename} with Submission ID {sub.id} as retrieved")
            indico_wrapper.mark_retreived(sub)
        logging.warning(f"Time:{(datetime.datetime.now() - begin_time).seconds} seconds") 
        logging.warning("All rejected submissions have been marked retrieved")
    # Exporting Exception files and their Submission IDs as a CSV
    exception_filepath = os.path.join(EXPORT_DIR, EXCEPTION_FILENAME)
    exceptions_df.to_csv(exception_filepath, index=False)
    logging.warning(f"Time:{(datetime.datetime.now() - begin_time).seconds} seconds")
    logging.warning("An exception file has been generated")
    print(f"Generated exceptions {exception_filepath}")
    print("An exception file has been generated")
