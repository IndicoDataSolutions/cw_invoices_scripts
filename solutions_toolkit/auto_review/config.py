import yaml


# TODO: need to do config checks for error catching
class Configuration:
    def __init__(self, config):
        self.host = config.get("host")
        self.api_token_path = config.get("api_token_path")
        self.workflow_id = config.get("workflow_id")
        self.model_name = config.get("model_name")

    @classmethod
    def from_yaml(cls, yaml_filepath):
        with open(yaml_filepath) as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
        return cls(config)
