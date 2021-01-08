from collections import defaultdict

from auto_review_functions import (
    accept_by_confidence,
    reject_by_confidence,
    reject_by_character_length,
)


REVIEWERS = {
    "accept_by_confidence": accept_by_confidence,
    "reject_by_confidence": reject_by_confidence,
    "reject_by_character_length": reject_by_character_length,
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
            for fn_config in review_fn_configs:
                if 