from collections import defaultdict

from solutions_toolkit.auto_review.auto_review_functions import (
    accept_by_confidence,
    reject_by_confidence,
    reject_by_character_length,
    accept_all_by_confidence,
    split_merged_values
)


REVIEWERS = {
    "accept_by_confidence": accept_by_confidence,
    "reject_by_confidence": reject_by_confidence,
    "reject_by_character_length": reject_by_character_length,
    "accept_all_by_confidence": accept_all_by_confidence,
    "split_merged_values": split_merged_values
}


class Reviewer:
    def __init__(self, predictions, model_name, review_config):
        self.review_config = review_config
        self.model_name = model_name
        self.predictions = predictions[self.model_name]
        self.prediction_label_map = self.format_pred_label_map()

    def format_pred_label_map(self):
        prediction_label_map = defaultdict(list)
        for pred in self.predictions:
            label = pred["label"]
            prediction_label_map[label].append(pred)
        return prediction_label_map

    def apply_reviews(self):
        for label, fn_configs in self.review_config.items():
            for fn_config in fn_configs:
                fn_name = fn_config["function"]
                review_fn = REVIEWERS[fn_name]
                kwargs = fn_config["kwargs"]
                if fn_config["prediction_set"] == "single":
                    updated_predictions = []
                    for pred in self.prediction_label_map[label]:
                        updated_pred = review_fn(pred, **kwargs)
                        updated_predictions.append(updated_pred)
                    self.prediction_label_map[label] = updated_predictions
                elif fn_config["prediction_set"] == "all":
                    updated_predictions = review_fn(
                        self.prediction_label_map[label], **kwargs
                    )
                    self.prediction_label_map[label] = updated_predictions

    def get_updated_predictions(self):
        updated_predictions = defaultdict(list)
        for pred_list in self.prediction_label_map.values():
            updated_predictions[self.model_name].extend(pred_list)
        updated_predictions[self.model_name] = sorted(
            updated_predictions[self.model_name], key=lambda x: x["start"]
        )
        return updated_predictions
