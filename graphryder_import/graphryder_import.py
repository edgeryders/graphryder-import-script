import utils
import json
import sql_discourse_import.get_data as get_data
import sql_discourse_import.load_data as load_data
import graph.graph as graph

# Initializing logs
log = utils.init_logs()

# Loading configuration
with open("./config.json") as json_config:
    config = json.load(json_config)

dbs = config['databases']
if config['reload_from_database']:
    get_data.get_from_dbs(log, config)
data = load_data.load_from_files(log, dbs)

graph.create(log, config, data)