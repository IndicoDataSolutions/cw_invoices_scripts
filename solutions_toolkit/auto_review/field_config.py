from solutions_toolkit.configuration import Configuration

FIELD_CONFIG = "FIELD_CONFIG"


# TODO: need to do config checks for error catching
class FieldConfiguration(Configuration):
    def __init__(self, config):
        self.field_config = {}
        for class_name, function_configs in config[FIELD_CONFIG].items():
            self.field_config[class_name] = function_configs
