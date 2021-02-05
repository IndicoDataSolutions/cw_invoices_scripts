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
    "Check Amount": [
        {
            "function": "remove_by_confidence",
            "kwargs": {
                "label": "Check Amount",
                "conf_threshold": 0.98
            },
            "prediction_set": "all",
        },
        {
            "function": "split_merged_values",
            "kwargs": {},
            "prediction_set": "all",
        },
    ],
    "Check Date": [
        {
            "function": "remove_by_confidence",
            "kwargs": {
                "label": "Check Date",
                "conf_threshold": 0.98
            },
            "prediction_set": "all",
        },
        {
            "function": "split_merged_values",
            "kwargs": {},
            "prediction_set": "all",
        },
    ],
    "Check Number": [
        {
            "function": "remove_by_confidence",
            "kwargs": {
                "label": "Check Number",
                "conf_threshold": 0.98
            },
            "prediction_set": "all",
        },
        {
            "function": "split_merged_values",
            "kwargs": {},
            "prediction_set": "all",
        },
    ],
    "Deposit Amount": [
        {
            "function": "remove_by_confidence",
            "kwargs": {
                "label": "Deposit Amount",
                "conf_threshold": 0.98
            },
            "prediction_set": "all",
        },
        {
            "function": "split_merged_values",
            "kwargs": {},
            "prediction_set": "all",
        },
    ],
    "Deposit Date": [
        {
            "function": "remove_by_confidence",
            "kwargs": {
                "label": "Deposit Date",
                "conf_threshold": 0.98
            },
            "prediction_set": "all",
        },
        {
            "function": "split_merged_values",
            "kwargs": {},
            "prediction_set": "all",
        },
    ],
    "Withdrawal Amount": [
        {
            "function": "remove_by_confidence",
            "kwargs": {
                "label": "Withdrawal Amount",
                "conf_threshold": 0.98
            },
            "prediction_set": "all",
        },
        {
            "function": "split_merged_values",
            "kwargs": {},
            "prediction_set": "all",
        },
    ],
    "Withdrawal Date": [
        {
            "function": "remove_by_confidence",
            "kwargs": {
                "label": "Withdrawal Date",
                "conf_threshold": 0.98
            },
            "prediction_set": "all",
        },
        {
            "function": "split_merged_values",
            "kwargs": {},
            "prediction_set": "all",
        },
    ],
    "Ending Balance": [
        {
            "function": "remove_by_confidence",
            "kwargs": {
                "label": "Ending Balance",
                "conf_threshold": 0.98
            },
            "prediction_set": "all",
        },
    ]
}
