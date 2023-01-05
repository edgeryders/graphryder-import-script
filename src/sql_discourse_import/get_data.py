import sys
sys.path.append('../')
import os
import json
import psycopg2
import utils

# init dict for data
data = {}

# Salt for hashing redacted data that is needed for matching, like user emails
salt = os.urandom(32)

def get_data(
        config,
        log,
        db_cursor, 
        db_name, 
        db_root, 
        ensure_consent, 
        salt,
        protected_topic_policy, 
        pseudonymize_users, 
        omit_codes_prefix, 
        omitted_projects
        ):

    if db_root:
        db_root = db_root + '.'

    data = {}

    import sql_discourse_import.discourse_queries.site_settings as site_settings
    data['stats'] = {}
    data['site'] = site_settings.get(log, db_root, db_cursor, db_name)

    import sql_discourse_import.discourse_queries.users as users
    data['users'] = users.get(log, db_root, db_cursor, salt, config)

    import sql_discourse_import.discourse_queries.groups as groups
    data['groups'] = groups.get(log, db_root, db_cursor)

    import sql_discourse_import.discourse_queries.categories as categories
    data['categories'] = categories.get(log, db_root, db_cursor)

    import sql_discourse_import.discourse_queries.tags as tags
    data['tags'] = tags.get(log, db_root, db_cursor)

    import sql_discourse_import.discourse_queries.topics as topics
    data['topics'], lost_topics, pm_topic_set, pm_count, topic_tags_data = topics.get(log, db_root, db_cursor, data['categories'], data['users'], ensure_consent)

    import sql_discourse_import.discourse_queries.posts as posts
    data['posts'], private_count, pm_topic_set = posts.get(log, db_root, db_cursor, data['topics'], data['users'], ensure_consent)

    import sql_discourse_import.discourse_queries.quotes as quotes
    data['quotes'] = quotes.get(log, db_root, db_cursor, data['posts'])

    import sql_discourse_import.discourse_queries.replies as replies
    data['replies'] = replies.get(log, db_root, db_cursor, data['posts'])

    import sql_discourse_import.discourse_queries.likes as likes
    data['likes'] = likes.get(log, db_root, db_cursor, data['posts'])

    import sql_discourse_import.annotator_queries.languages as languages
    data['languages'], language_list = languages.get(log, db_root, db_cursor)

    import sql_discourse_import.annotator_queries.projects as projects
    data['projects'], project_list = projects.get(log, db_root, db_cursor, omitted_projects)

    import sql_discourse_import.annotator_queries.codes as codes
    data['codes'] = codes.get(log, db_root, db_cursor)

    import sql_discourse_import.annotator_queries.code_names as code_names
    data['code_names'] = code_names.get(log, db_root, db_cursor)

    import sql_discourse_import.annotator_queries.annotations as annotations
    data['annotations'] = annotations.get(log, db_root, db_cursor, ensure_consent, data['posts'])

    if protected_topic_policy == 'omit':
        omit_protected_content = True
    else:
        omit_protected_content = False
    if protected_topic_policy == 'redact':
        redact_protected_content = True
    else: 
        redact_protected_content = False

    import sql_discourse_import.redactions as redactions
    data = redactions.redact(log, data, pseudonymize_users, omit_protected_content, redact_protected_content, omit_codes_prefix)

    data['stats'] = {
        'omit_pm': True,
        'omit_system_users': True,
        'omit_protected': omit_protected_content,
        'users': len(data['users'].keys()),
        'groups': len(data['groups'].keys()),
        'tags': len(data['tags'].keys()),
        'categories': len(data['categories'].keys()),
        'topics': len(data['topics'].keys()),
        'pm_threads': pm_count,
        'topics_by_deleted_users': len(lost_topics),
        'tags_applied': len(topic_tags_data),
        'posts': len(data['posts'].keys()),
        'messages': private_count,
        'annotator_languages': language_list[:-1],
        'annotator_projects': project_list[:-1],
        'annotator-codes': len(list(data['codes'].keys())),
        'annotator-code-names': len(list(data['code_names'].keys())),
        'annotator-annotations': len(list(data['annotations'].keys()))
    }

    return data

def get_from_dbs(log, config):

    tables = [
        'users',
        'groups',
        'tags',
        'categories',
        'topics',
        'posts',
        'replies',
        'quotes',
        'likes',
        'languages',
        'projects',
        'codes',
        'code_names',
        'annotations'
        ]

    # Ensuring directory for local database dump files
    data_path = './db'
    try:
        os.mkdir(data_path)
    except OSError:
        print ("Database directory %s" % data_path)
    else:
        print ("Successfully created the directory %s " % data_path)
    data_path = os.path.abspath('./db/')

    # Load data from Discourse psql databases and dump to json files
    # Data is loaded from JSON files because Neo4j APOC functions are optimized for this.
    dbs = config['databases'][:]

    for db in dbs:
        db_conn = psycopg2.connect(
            host=db['host'], 
            port=db['port'], 
            dbname=db['dbname'], 
            user=db['user'], 
            password=db['password'],
        )

        # Open a cursor to perform database operations
        db_cursor = db_conn.cursor()

        # Get data
        d = get_data(
                    config,
                    log,
                    db_cursor, 
                    db['name'], 
                    db['database_root'], 
                    db['ensure_consent'] == 'true', 
                    salt, 
                    db['protected_topic_policy'], 
                    db['pseudonymize_users'] == 'true',
                    db['omit_codes_prefix'],
                    db['omitted_projects']
                    ) 
        data[db['name']] = d
        stats = d['stats']
        stats['chunk_sizes'] = {}

        # Site data is always a single object
        with open(f'./db/{db["name"]}_site.json', 'w') as file:
            json.dump(d['site'], file, default=str)

        for table in tables:
            stats = utils.dumpSplit(table, db['name'], list(d[table].values()), stats)

        # Add chunk sizes to stats last
        with open(f'./db/{db["name"]}_stats.json', 'w') as file:
            json.dump(stats, file, default=str)