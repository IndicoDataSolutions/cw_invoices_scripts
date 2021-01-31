import yaml


# TODO: need to do config checks for error catching
class Configuration:
    def __init__(self, config):
        pass

    @classmethod
    def from_yaml(cls, yaml_filepath):
        with open(yaml_filepath) as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
        return cls(config)
