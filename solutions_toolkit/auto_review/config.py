from solutions_toolkit.configuration import Configuration


# TODO: need to do config checks for error catching
class AutoReviewConfiguration(Configuration):
    def __init__(self, config):
        self.host = config.get("host")
        self.api_token_path = config.get("api_token_path")
        self.workflow_id = config.get("workflow_id")
        self.model_name = config.get("model_name")
