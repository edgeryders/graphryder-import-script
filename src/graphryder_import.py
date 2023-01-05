import utils
import json
import sql_discourse_import.get_data as get_data
import sql_discourse_import.load_data as load_data
import graph.graph as graph
from pprint import pprint

# Initializing logs
log = utils.init_logs()

# Loading configuration
with open("./config.json") as json_config:
    config_file = json.load(json_config)

with open("./config.default.json") as json_config:
    default = json.load(json_config)

# Initializing default config values
config = {'databases': [{}]}
config['reload_from_database'] = default['reload_from_database'] if not 'reload_from_database' in config_file else config_file['reload_from_database']
config['redact_emails'] = default['redact_emails'] if not 'redact_emails' in config_file else config_file['redact_emails']
config['neo4j_uri'] = default['neo4j_uri'] if not 'neo4j_uri' in config_file else config_file['neo4j_uri']
config['neo4j_user'] = default['neo4j_user'] if not 'neo4j_user' in config_file else config_file['neo4j_user']
config['neo4j_password'] = default['neo4j_password'] if not 'neo4j_password' in config_file else config_file['neo4j_password']

dbs = default['databases'] if not 'databases' in config_file else config_file['databases']
for i, database in enumerate(dbs):
    config['databases'][i]['name'] = default['databases'][0]['name'] if not 'name' in database else config_file['databases'][i]['name']
    config['databases'][i]['ensure_consent'] = default['databases'][0]['ensure_consent'] if not 'ensure_consent' in database else config_file['databases'][i]['ensure_consent']
    config['databases'][i]['protected_topic_policy'] = default['databases'][0]['protected_topic_policy'] if not 'protected_topic_policy' in database else config_file['databases'][i]['protected_topic_policy']
    config['databases'][i]['pseudonymize_users'] = default['databases'][0]['pseudonymize_users'] if not 'pseudonymize_users' in database else config_file['databases'][i]['pseudonymize_users']
    config['databases'][i]['database_root'] = default['databases'][0]['database_root'] if not 'database_root' in database else config_file['databases'][i]['database_root']
    config['databases'][i]['omit_codes_prefix'] = default['databases'][0]['omit_codes_prefix'] if not 'omit_codes_prefix' in database else config_file['databases'][i]['omit_codes_prefix']
    config['databases'][i]['host'] = default['databases'][0]['host'] if not 'host' in database else config_file['databases'][i]['host']
    config['databases'][i]['port'] = default['databases'][0]['port'] if not 'port' in database else config_file['databases'][i]['port']
    config['databases'][i]['dbname'] = default['databases'][0]['dbname'] if not 'dbname' in database else config_file['databases'][i]['dbname']
    config['databases'][i]['user'] = default['databases'][0]['user'] if not 'user' in database else config_file['databases'][i]['user']
    config['databases'][i]['password'] = default['databases'][0]['password'] if not 'password' in database else config_file['databases'][i]['password']
    config['databases'][i]['omitted_projects'] = default['databases'][0]['omitted_projects'] if not 'omitted_projects' in database else config_file['databases'][i]['omitted_projects']

# Import data from Discourse if necessecary and load it to memory
if config['reload_from_database']:
    get_data.get_from_dbs(log, config)
data = load_data.load_from_files(log, config['databases'])

# Build graph
graph.create(log, config, data)