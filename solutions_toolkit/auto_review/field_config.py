"""
Field config configures the auto review functions that will apply to each class
in a document's prediction

It has the srtructure
{
    CLASS_NAME: [
        {
            "function": FUNCTION NAME,
            "kwargs": {FUCNTION KEY WORDS},
            "prediction_set": "single" or "all" ----> This determines whether the function works on the full label set or one at a time
        }
        .
        .
        .
    ]
}

Note that the functions you apply need to have an appropriate mapping in
the REVIEWERS dictionary in reviewer.py
"""

FIELD_CONFIG = {
    "Remit to Address (State)": [
        {
            "function": "accept_by_confidence",
            "kwargs": {
                "label": "Remit to Address (State)",
                "conf_threshold": 0.98
            },
            "prediction_set": "single",
        },
        {
            "function": "reject_by_confidence",
            "kwargs": {
                "label": "Remit to Address (State)",
                "conf_threshold": 0.5
            },
            "prediction_set": "single",
        }
    ],
    "Supplier Name": [
        {
            "function": "accept_all_by_confidence",
            "kwargs": {
                "label": "Supplier Name",
                "conf_threshold": 0.98
            },
            "prediction_set": "all",
        },
        {
            "function": "reject_by_confidence",
            "kwargs": {
                "label": "Supplier Name",
                "conf_threshold": 0.5
            },
            "prediction_set": "single",
        },
        {
            "function": "reject_by_character_length",
            "kwargs": {
                "length_threshold": 3
            },
            "prediction_set": "single",
        },
    ]
}
