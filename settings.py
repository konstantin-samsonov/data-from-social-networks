# Project configuration file

import os
import tomllib

path_config_file = f"{os.getenv('HOME')}/credentials/dfsn-config.toml"

with open(path_config_file, "rb") as f:
    config = tomllib.load(f)

BIGQUERY = config['bigquery']