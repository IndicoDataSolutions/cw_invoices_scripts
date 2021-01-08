from collections import defaultdict

from auto_review_functions import (
    accept_by_confidence,
    reject_by_confidence,
    reject_by_character_length,
    accept_all_by_confidence,
)


REVIEWERS = {
    "accept_by_confidence": accept_by_confidence,
    "reject_by_confidence": reject_by_confidence,
    "reject_by_character_length": reject_by_character_length,
    "accept_all_by_confidence": accept_all_by_confidence
}


class Reviewer:
    def __init__(self, predictions, review_config):
        self.review_config = review_config
        self.predictions = predictions
        self.prediction_label_map = self.format_preds()

    def format_preds(self):
        prediction_label_map = defaultdict(list)
        for pred in self.predictions:
            label = pred['label']
            prediction_label_map[label].append(pred)
        return prediction_label_map

    def apply_reviews(self):
        for label, fn_configs in self.review_config:
            for fn_config in fn_configs:
                fn_name = fn_config["function"]
                kwargs = fn_config["kwargs"]
                if fn_config["predictions_set"] == "single":
                    if fn_config["label_required"]:
                        review_fn = REVIEWERS[fn_name]
                        updated_predictions = []
                        for pred in self.prediction_label_map[label]:
                            updated_pred = review_fn(pred, label, **kwargs)
                            updated_predictions.append(updated_pred)
                        self.prediction_label_map[label] = updated_predictions
