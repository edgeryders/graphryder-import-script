import psycopg2
import logging
import json
import hashlib
import random
import os
from neo4j import GraphDatabase
import numpy as np
from gibberish import Gibberish

# Python version 3.8.6
# For this script to work, neo4j must have APOC installed and the neo4j.conf file 
# must have the following properties set:
# apoc.import.file.enabled=true
# apoc.import.file.use_neo4j_config=false

mylogs = logging.getLogger(__name__)
mylogs.setLevel(logging.DEBUG)

file = logging.FileHandler("ryderex-import.log")
file.setLevel(logging.INFO)
fileformat = logging.Formatter("%(asctime)s:%(levelname)s: %(message)s",datefmt="%H:%M:%S")
file.setFormatter(fileformat)

stream = logging.StreamHandler()
stream.setLevel(logging.DEBUG)
streamformat = logging.Formatter("%(asctime)s: %(message)s")
stream.setFormatter(streamformat)

mylogs.addHandler(file)
mylogs.addHandler(stream)

with open("./config.json") as json_config:
    config = json.load(json_config)

uri = config['neo4j_uri']
driver = GraphDatabase.driver(uri, auth=(config['neo4j_user'], config['neo4j_password']))
data_path = os.path.abspath('./db/')

# Save data in chunks of size n
def dumpSplit(data_topic, data_set, data, stats):
    path = './db/'
    n = 1000
    data_chunks = [data[i * n:(i + 1) * n] for i in range((len(data) + n - 1) // n )]
    for num, item in enumerate(data_chunks):
        with open(f'{path}{data_set}_{data_topic}_{str(num+1)}.json', 'w') as file:
            json.dump(item, file, default=str)
    stats['chunk_sizes'][data_topic] = len(data_chunks)
    return stats

def get_data(db_cursor, db_name, db_root, salt, ensure_consent, protected_topic_policy, pseudonymize_users, omit_codes_prefix):
    # This function gets the data we need from the Discourse psql database.
    # It assumes that the database is built from backup dumps. 
    # If running on the live database, 'backup' in the database names should be changed.
    # TODO: make the database name into a variable to enable loading from backup or live db

    mylogs.info(f'Loading new data from {db_name}')

    # Get site data

    if db_root:
        db_root = db_root + '.'

    site_query = f'''
    SELECT 
    value 
    FROM {db_root}site_settings 
    WHERE name = 'vapid_base_url' 
    LIMIT 1 
    '''

    db_cursor.execute(site_query)
    site_data = db_cursor.fetchall()
    site = {
        'name': db_name,
        'url': site_data[0][0]
    }
    mylogs.info(f'    Loading data from {site["url"]} database...')

    # Get users, consent, group memberships

    users_query = f'''
    SELECT
    users.id, username_lower, email
    FROM {db_root}users AS users, {db_root}user_emails as emails
    WHERE users.id = emails.user_id;
    '''

    consent_query = f'''
    SELECT 
    user_id, value, updated_at 
    FROM {db_root}user_custom_fields 
    WHERE name = 'edgeryders_consent';
    '''

    group_members_query = f'''
    SELECT
    group_id, user_id
    FROM {db_root}group_users
    '''

    users = {}
    db_cursor.execute(users_query)
    users_data = db_cursor.fetchall()
    # If emails are redacted, we use a salted hash to link user accounts together
    # Since we need the same email to return the same hash, we use one salt for all platforms and users. 
    if config['redact_emails']:
        mylogs.info(f'    Redacting emails and replacing with hashes...')
    for user in users_data:
        uid = user[0]
        email = user[2]
        hashed_email = hashlib.pbkdf2_hmac(
            'sha256',
            email.encode('utf-8'),
            salt,
            1000,
            dklen=128
        )
        if config['redact_emails']:
            email = hashed_email
        users[uid] = {
            'id': uid,
            'username': user[1],
            'email': email,
            'groups': [],
            'consent': 0,
            'consent_updated': 0
        }
    
    # Adding another system user as a dummy user for private and deleted content
    users[-100] = {
        'id': -100,
        'username': "Unknown",
        'email': "Unknown",
        'groups': [],
        'consent': 0,
        'consent_updated': 0
    }

    db_cursor.execute(consent_query)
    consent_data = db_cursor.fetchall()
    for user in consent_data:
        uid = user[0]
        if user[1] == '1':
            users[uid]['consent'] = 1
        else:
            users[uid]['consent'] = 0
        users[uid]['consent_updated'] = user[2]

    db_cursor.execute(group_members_query)
    group_members_data = db_cursor.fetchall()
    for group_member in group_members_data:
        uid = group_member[1]
        users[uid]['groups'].append(group_member[0])

    mylogs.info(f'    Got {len(users.keys())} users')

    # Get groups

    groups_query = f'''
    SELECT 
    id, name, visibility_level 
    FROM {db_root}groups
    '''

    groups = {}
    db_cursor.execute(groups_query)
    group_data = db_cursor.fetchall()
    for group in group_data:
        gid = group[0]
        groups[gid] = {
            'id': gid,
            'name': group[1],
            'visibility_level': group[2]
        }

    mylogs.info(f'    Got {len(groups.keys())} groups')

    # Get categories

    categories_query = f'''
    SELECT
    id, name, name_lower, created_at, updated_at, read_restricted, parent_category_id
    FROM {db_root}categories
    '''

    categories_permissions = f'''
    SELECT
    id, category_id, group_id, permission_type
    FROM {db_root}category_groups
    '''

    categories = {}
    db_cursor.execute(categories_query)
    category_data = db_cursor.fetchall()
    for category in category_data:
        cid = category[0]
        categories[cid] = {
            'id': cid,
            'name': category[1], 
            'name_lower': category[2],
            'created_at': category[3], 
            'updated_at': category[4], 
            'read_restricted': category[5], 
            'parent_category_id': category[6],
            'permissions': []
        }

    # Group 0 is 'everyone' and permission_type is an integer 1 = Full 2 = Reply and read 3 = Read Only
    db_cursor.execute(categories_permissions)
    category_permission_data = db_cursor.fetchall()
    for permission in category_permission_data:
        cid = permission[1]
        categories[cid]['permissions'].append(permission[2])

    mylogs.info(f'    Got {len(categories.keys())} categories')

    # Get tags

    tags_query = f'''
    SELECT
    id, name, topic_count, created_at, updated_at
    FROM {db_root}tags
    '''

    tags = {}
    db_cursor.execute(tags_query)
    tags_data = db_cursor.fetchall()
    for tag in tags_data:
        tid = tag [0]
        tags[tid] = {
            'id': tid,
            'name': tag[1],
            'topic_count': tag[2],
            'created_at': tag[3],
            'updated_at': tag[4]
        }

    mylogs.info(f'    Got {len(tags.keys())} tags.')

    # Get topics, permissions, topic tags
    # Private messages are excluded

    topics_query = f'''
    SELECT
    id, title, created_at, updated_at, user_id, category_id
    FROM {db_root}topics
    '''

    allowed_users_query = f'''
    SELECT 
    topic_id, user_id
    FROM {db_root}topic_allowed_users
    '''

    topic_tags_query = f'''
    SELECT
    topic_id, tag_id
    FROM {db_root}topic_tags
    '''

    pm_count = 0
    pm_topic_set = set()
    db_cursor.execute(allowed_users_query)
    allowed_users_data = db_cursor.fetchall()
    for permission in allowed_users_data:
        tid = permission[0]
        pm_topic_set.add(tid)
        pm_count += 1

    topics = {}
    db_cursor.execute(topics_query)
    topics_data = db_cursor.fetchall()
    lost_topics = set()
    for topic in topics_data:
        tid = topic[0]
        cid = topic[5] if topic[5] in categories.keys() else None
        read_restricted = True if tid in pm_topic_set or not cid else categories[cid]['read_restricted']
        if topic[4] in users.keys() and users[topic[4]]['consent'] == 1:
            consenting = True
        else:
            consenting = False
        if consenting and tid in pm_topic_set:
            title = 'Private message'
        if not consenting and ensure_consent:
            title = "Title withdrawn (no user consent)"
        else:
            title = topic[1]
        topics[tid] = {
            'id': tid,
            'title': title,
            'created_at': topic[2],
            'updated_at': topic[3],
            'user_id': -100 if tid in pm_topic_set or topic[4] not in users.keys() else topic[4],
            'is_message_thread': True if tid in pm_topic_set else False,
            'category_id': cid,
            'read_restricted': read_restricted,
            'allowed_users': [],
            'tags': []
        }
        if topic[4] not in users.keys():
            lost_topics.add(tid)

    db_cursor.execute(topic_tags_query)
    topic_tags_data = db_cursor.fetchall()
    for tag in topic_tags_data:
        tid = tag[0]
        topics[tid]['tags'].append(tag[1])

    mylogs.info(f'    Got {len(topics.keys())} topics and applied {len(topic_tags_data)} tags.')

    # Get posts
    # Private messages are excluded

    posts_query = f'''
    SELECT
    id, user_id, topic_id, post_number, raw, created_at, updated_at, deleted_at, hidden, word_count, wiki, reads, score, like_count, reply_count, quote_count
    FROM {db_root}posts
    '''

    replies_query = f'''
    SELECT
    post_id, reply_post_id
    FROM {db_root}post_replies
    '''

    quotes_query = f'''
    SELECT
    post_id, quoted_post_id
    FROM {db_root}quoted_posts
    '''

    likes_query = f'''
    SELECT
    post_id, user_id
    FROM {db_root}post_actions
    WHERE post_action_type_id = 2
    '''

    posts = {}
    pm_post_set = set()
    private_count = 0
    db_cursor.execute(posts_query)
    posts_data = db_cursor.fetchall()
    for post in posts_data:
        pid = post[0]
        tid = post[2]
        private = True if post[2] in pm_topic_set else False
        read_restricted = True if tid not in topics.keys() else topics[tid]['read_restricted']
        if private:
            pm_post_set.add(pid)
            private_count += 1
        deleted = post[7]

        if post[1] in users.keys() and users[post[1]]['consent'] == 1:
            consenting = True
        else:
            consenting = False

        if consenting and deleted:
            raw = 'Removed content (deleted)'
        if consenting and private:
            raw = 'Removed content (private message)'
        if not consenting and ensure_consent:
            raw = "Content withdrawn (no user consent)"
        else:
            raw = post[4]

        posts[pid] = {
            'id': pid,
            'user_id': -100 if private or deleted or post[1] not in users.keys() else post[1],
            'topic_id': tid,
            'post_number': post[3],
            'raw': raw,
            'created_at': post[5],
            'updated_at': post[6],
            'deleted_at': post[7],
            'hidden': post[8],
            'read_restricted': read_restricted,
            'word_count': 0 if private or deleted else post[9],
            'wiki': post[10],
            'reads': 0 if private or deleted else post[11],
            'score': 0 if private or deleted else post[12],
            'like_count': 0 if private or deleted else post[13],
            'reply_count': post[14],
            'quote_count': post[15],
            'quotes_posts': [],
            'is_reply_to': [],
            'is_liked_by': [],
            'is_private': private,
            'consent': consenting
        }

    quotes = {}
    db_cursor.execute(quotes_query)
    quotes_data = db_cursor.fetchall()
    for num, quote in enumerate(quotes_data):
        posts[quote[0]]['quotes_posts'].append(quote[1])
        quotes[num] = {
            'id': num,
            'post_id': quote[0],
            'quoted_post_id': quote[1]
        }

    replies = {}
    db_cursor.execute(replies_query)
    replies_data = db_cursor.fetchall()
    for num, reply in enumerate(replies_data):
        posts[reply[1]]['is_reply_to'].append(reply[0])
        replies[num] = {
            'id': num,
            'post_id': reply[0],
            'reply_post_id': reply[1]
        }

    likes = {}
    db_cursor.execute(likes_query)
    likes_data = db_cursor.fetchall()
    for num, like in enumerate(likes_data):
        posts[like[0]]['is_liked_by'].append(like[1])
        likes[num] = {
            'id': num,
            'post_id': like[0],
            'user_id': like[1]
        }

    mylogs.info(f'    Got {len(posts.keys())} posts.')
    mylogs.info(f'    Got {len(replies.keys())} replies.')
    mylogs.info(f'    Got {len(quotes.keys())} quotes.')
    mylogs.info(f'    Got {len(likes.keys())} likes.')

    # Get annotator languages

    annotator_languages_query = f'''
    SELECT
    id, name, locale
    FROM {db_root}discourse_annotator_languages
    '''

    annotator_languages = {}
    language_list = ''
    db_cursor.execute(annotator_languages_query)
    annotator_languages_data = db_cursor.fetchall()
    for language in annotator_languages_data:
        lid = language[0]
        annotator_languages[lid] = {
            'id': lid,
            'name': language[1],
            'locale': language[2]
        }
        language_list += f' {language[1]},'

    mylogs.info(f'    Got annotation languages:{language_list[:-1]}.') 

    # Get annotator codes and code names

    annotator_codes_query = f'''
    SELECT
    id, description, creator_id, created_at, updated_at, ancestry, annotations_count
    FROM {db_root}discourse_annotator_codes
    '''

    annotator_code_names_query = f'''
    SELECT
    id, name, code_id, language_id, created_at
    FROM {db_root}discourse_annotator_code_names
    '''

    annotator_codes = {}
    db_cursor.execute(annotator_codes_query)
    annotator_codes_data = db_cursor.fetchall()
    for code in annotator_codes_data:
        cid = code[0]
        annotator_codes[cid] = {
            'id': cid,
            'description': code[1],
            'creator_id': code[2],
            'created_at': code[3],
            'updated_at': code[4],
            'ancestry': code[5],
            'annotations_count': code[6]
        }

    annotator_code_names = {}
    db_cursor.execute(annotator_code_names_query)
    annotator_code_names_data = db_cursor.fetchall()
    for name in annotator_code_names_data:
        nid = name[0]
        annotator_code_names[nid] = {
            'id': nid,
            'name': name[1],
            'code_id': name[2],
            'language_id': name[3],
            'created_at':name[4]
        }

    mylogs.info(f'    Got {len(list(annotator_codes.keys()))} codes with {len(list(annotator_code_names.keys()))} names.')

    # Get annotator annotations and ranges

    annotations_query = f'''
    SELECT
    id, text, quote, created_at, updated_at, code_id, post_id, creator_id, type, topic_id
    FROM {db_root}discourse_annotator_annotations
    '''

    annotations = {}
    db_cursor.execute(annotations_query)
    annotations_data = db_cursor.fetchall()
    for annotation in annotations_data:
        if not ensure_consent:
            include = True
        elif annotation[6] in posts.keys() and posts[annotation[6]]['consent'] == True:
            include = True
        else:
            include = False
        if include:
            aid = annotation[0]
            annotations[aid] = {
                'id': aid,
                'text': annotation[1], 
                'quote': annotation[2], 
                'created_at': annotation[3],
                'updated_at': annotation[4], 
                'code_id': annotation[5],
                'post_id': annotation[6], 
                'creator_id': annotation[7], 
                'type': annotation[8],
                'topic_id': annotation[9] 
        }

    mylogs.info(f'    Got {len(list(annotations.keys()))} annotations.')

    # Omit data
    # We accept the redundancy and inefficency of looping through the data
    # and removing records as this makes the code less complicated
    # and makes it easier to add new rules later for how and when to omit data
    # TODO: Move these bools to a config file
    
    omit_private_messages = True
    # Omit private messages from the graph

    if protected_topic_policy == 'omit':
        omit_protected_content = True
    else:
        omit_protected_content = False
    
    if protected_topic_policy == 'redact':
        redact_protected_content = True
    else: 
        redact_protected_content = False

    # Omit protected content (posts, categories, groups) from the graph.
    # Content that is not readable by all logged in users is considered protected.
    # This also omits 'hidden' posts, also known as 'whispers'.
    # In the future, we may want to handle 'hidden' posts separately if we 
    # if we want to give access to the graph based on the permissions of a loggen in user.

    omit_system_users = True
    # This omits content created by system users and by deleted users.
    # It also omits those users completely from the graph.

    new = dict(users)
    pseudonyms = []
    gib = Gibberish()
    if pseudonymize_users:
        pseudonym_list = gib.generate_words(max(users.keys()) + 1)
    for u, d in users.items():
        if omit_system_users and d['id'] < 0:
            del(new[u])
            continue
        if pseudonymize_users:
            name = pseudonym_list[u]
            while name in pseudonyms:
                name = name + '_' + random.choice(pseudonym_list)
            pseudonyms.append(name)
            new[u]['username'] = name
    users = new

    new = dict(groups)
    # Group visibility levels, public=0, logged_on_users=1, members=2, staff=3, owners=4
    for g, d in groups.items():
        if redact_protected_content and d['visibility_level'] > 1:
            new[g]['name'] = '[Redacted]'
            continue
        if omit_protected_content and d['visibility_level'] > 1:
            del(new[g])
            continue
    groups = new

    new = dict(categories)
    for c, d in categories.items():
        if redact_protected_content and d['read_restricted']:
            new[c]['name'] = '[Redacted]'
            new[c]['name'] = '[Redacted]'
            continue
        if omit_protected_content and d['read_restricted']:
            del(new[c])
            continue
        for group in d['permissions']:
            if group not in groups.keys():
                new[c]['permissions'].remove(group)
    categories = new

    new = dict(topics)
    for t, d in topics.items():
        if omit_private_messages and t in pm_topic_set:
            del(new[t])
            continue
        if redact_protected_content and d['read_restricted']:
            new[t]['title'] = '[Redacted]'
            continue
        if omit_protected_content and d['read_restricted']:
            del(new[t])
            continue
    topics = new

    new = dict(posts)
    for p, d in posts.items():
        if omit_private_messages and p in pm_post_set:
            del(new[p])
            continue
        if omit_protected_content and d['hidden']:
            del(new[p])
            continue
        if redact_protected_content and d['read_restricted']:
            new[p]['raw'] = '[Redacted]'
        if omit_protected_content and (d['read_restricted'] or d['hidden']):
            del(new[p])
            continue
    posts = new

    new = dict(quotes)
    for q, d in quotes.items():
        if omit_private_messages and (d['quoted_post_id'] in pm_post_set or d['post_id'] in pm_post_set):
            del(new[q])
            continue
        if omit_protected_content and (d['quoted_post_id'] in pm_post_set or d['post_id'] not in posts.keys()):
            del(new[q])
            continue
    quotes = new
    
    new = dict(likes)
    for l, d in likes.items():
        if omit_private_messages and d['post_id'] in pm_post_set:
            del(new[l])
            continue
        if omit_protected_content and d['post_id'] not in posts.keys():
            del(new[l])
            continue
    likes = new

    new = dict(annotator_code_names)
    omitted = set()
    for a, d in annotator_code_names.items():
        if omit_codes_prefix:
            for prefix in omit_codes_prefix:
                if d['name'].startswith(prefix):
                    omitted.add(d['code_id'])
                    if d['code_id'] in annotator_codes.keys():
                        del[annotator_codes[d['code_id']]]
                    del[new[a]]
                    continue

    new = dict(annotations)
    for a, d in annotations.items():
        if d['code_id'] in omitted:
            del(new[a])
            continue
        if omit_private_messages and d['post_id'] in pm_post_set:
            del(new[a])
            continue
        if redact_protected_content and d['post_id'] not in posts.keys():
            new[a]['quote'] = '[Redacted]' 
        if omit_protected_content and d['post_id'] not in posts.keys():
            del(new[a])
            continue
    annotations = new

    if omit_private_messages:
        mylogs.info('Omitted private messages.')
    if omit_protected_content:
        mylogs.info('Omitted protected content.')
    if omit_system_users:
        mylogs.info('Omitted system users and content.')
    
    stats = {
        'omit_pm': omit_private_messages,
        'omit_protected': omit_protected_content,
        'omit_system_users': omit_system_users,
        'users': len(users.keys()),
        'groups': len(groups.keys()),
        'tags': len(tags.keys()),
        'categories': len(categories.keys()),
        'topics': len(topics.keys()),
        'pm_threads': pm_count,
        'topics_by_deleted_users': len(lost_topics),
        'tags_applied': len(topic_tags_data),
        'posts': len(posts.keys()),
        'messages': private_count,
        'annotator_languages': language_list[:-1],
        'annotator-codes': len(list(annotator_codes.keys())),
        'annotator-code-names': len(list(annotator_code_names.keys())),
        'annotator-annotations': len(list(annotations.keys()))
    }

    return {
        'stats': stats,
        'site': site,
        'users': users,
        'groups': groups,
        'tags': tags,
        'categories': categories,
        'topics': topics,
        'posts': posts,
        'replies': replies,
        'quotes': quotes,
        'likes': likes,
        'languages': annotator_languages,
        'codes': annotator_codes,
        'code_names': annotator_code_names,
        'annotations': annotations
    }

def reload_data(dbs):
    # This function triggers loading data from all databases it gets as input.
    # It outputs the data into chunked json files in the 'db' directory.
    # This is done as the APOC calls of Neo4j work best when loading from files in chunks.
    # Chunk size is 1000 records.
    # TODO: Set chunk size through parameter when loading script with reload flag.

    mylogs.info('Loading new data from databases...')
    db_path = './db'
    try:
        os.mkdir(db_path)
    except OSError:
        print ("Database directory %s" % db_path)
    else:
        print ("Successfully created the directory %s " % db_path)

    data = {}
    # Salt for hashing redacted data that is needed for matching, like user emails.    
    salt = os.urandom(32)
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
                    db_cursor, 
                    db['name'], 
                    db['database_root'], 
                    salt, db['ensure_consent'] == 'true', 
                    db['protected_topic_policy'], 
                    db['pseudonymize_users'] == 'true',
                    db['omit_codes_prefix']
                    ) 
        data[db['name']] = d
        stats = d['stats']
        stats['chunk_sizes'] = {}

        # Site data is always a single object
        with open(f'./db/{db["name"]}_site.json', 'w') as file:
            json.dump(d['site'], file, default=str)

        stats = dumpSplit('users', db['name'], list(d['users'].values()), stats)
        stats = dumpSplit('groups', db['name'], list(d['groups'].values()), stats)
        stats = dumpSplit('tags', db['name'], list(d['tags'].values()), stats)
        stats = dumpSplit('categories', db['name'], list(d['categories'].values()), stats)
        stats = dumpSplit('topics', db['name'], list(d['topics'].values()), stats)
        stats = dumpSplit('posts', db['name'], list(d['posts'].values()), stats)
        stats = dumpSplit('replies', db['name'], list(d['replies'].values()), stats)
        stats = dumpSplit('quotes', db['name'], list(d['quotes'].values()), stats)
        stats = dumpSplit('likes', db['name'], list(d['likes'].values()), stats)
        stats = dumpSplit('languages', db['name'], list(d['languages'].values()), stats)
        stats = dumpSplit('codes', db['name'], list(d['codes'].values()), stats)
        stats = dumpSplit('code_names', db['name'], list(d['code_names'].values()), stats)
        stats = dumpSplit('annotations', db['name'],list(d['annotations'].values()),  stats)

        # Add chunk sizes to stats last
        with open(f'./db/{db["name"]}_stats.json', 'w') as file:
            json.dump(stats, file, default=str)

def load_data(dbs):
    # This function is basically just a verification of that the data we need is in files in the db directory. 
    # TODO: Actually test data integrity before import?

    mylogs.info('')
    mylogs.info('Loading JSON data files to verify...')
    mylogs.info('')
    data = {}
    for db in dbs:
        data[db['name']] = {}
        with open(f'./db/{db["name"]}_site.json') as file:
            data[db['name']]['site'] = json.load(file)
        with open(f'./db/{db["name"]}_stats.json') as file:
            data[db['name']]['stats'] = json.load(file)
            stats = data[db['name']]['stats']

        for topic, chunks in stats['chunk_sizes'].items():
            data[db['name']][topic] = []
            for chunk in range(1, chunks + 1):
                with open(f'./db/{db["name"]}_{topic}_{chunk}.json') as file:
                    data[db['name']][topic].extend(json.load(file))

    for k,d in data.items():
        mylogs.info(f'-------| {k} |-------')
        mylogs.info(f'{len(d["users"])} users')
        mylogs.info(f'{len(d["groups"])} groups')
        mylogs.info(f'{len(d["tags"])} tags.')
        mylogs.info(f'{len(d["categories"])} tags.')
        mylogs.info(f'{len(d["topics"])} topics and {d["stats"]["pm_threads"]} PM threads.')
        mylogs.info(f'{d["stats"]["tags_applied"]} tag applications to topics.')
        mylogs.info(f'{len(d["posts"])} posts and {d["stats"]["messages"]} private messages.')
        mylogs.info(f'{len(d["replies"])} posts are replies to other posts.')
        mylogs.info(f'{len(d["quotes"])} quotes.')
        mylogs.info(f'{len(d["likes"])} likes.')
        
        if d['stats']['omit_pm']:
            mylogs.info('Private messages have been omitted.')
        else:
            mylogs.info('Private message content and message user identification has been omitted from the dataset.')

        if d['stats']['omit_protected']:
            mylogs.info('Protected content has been omitted.')

        if d['stats']['omit_system_users']:
            mylogs.info('System users and their content has been omitted.')

        mylogs.info(f'Annotation languages:{d["stats"]["annotator_languages"]}.') 
        mylogs.info(f'{len(d["codes"])} ethnographic codes with {len(d["code_names"])} names.')
        mylogs.info(f'{len(d["annotations"])} ethnographic annotations.')
        mylogs.info(' ')

    return data

def graph_clear():
    # Clear database function

    def tx_clear_neo4j(tx):
        tx.run(
            f'MATCH (a) DETACH DELETE a '
            )

    def tx_clear_fullTextIndexes(tx):
        tx.run(
            f'CALL db.index.fulltext.drop("cooccurrenceRelationshipIndex") '
            )

    with driver.session() as session:
        try:
            session.write_transaction(tx_clear_neo4j)
            session.write_transaction(tx_clear_fullTextIndexes)
            mylogs.info('Cleared database')
        except Exception as e:
            mylogs.error('Clearing database failed')
            mylogs.error(e)

def graph_create_platform(data):
    # Add platforms function

    def tx_create_platform(tx, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_site.json")'
            f'YIELD value '
            f'CREATE (p:platform {{url: value.url, name: "{dataset}"}})'
        )

    def tx_create_platform_index(tx):
        tx.run(
            f'CREATE INDEX platform IF NOT EXISTS '
            f'FOR (p:platform) '
            f'ON (p.name) '
        )

    with driver.session() as session:
        session.write_transaction(tx_create_platform_index)

    for platform in data.values():
        with driver.session() as session:
            try:
                session.write_transaction(tx_create_platform, platform['site']['name'])
                mylogs.debug(f'Loaded platform data from {platform["site"]["name"]}')
            except Exception as e:
                mylogs.error(f'Import failed for platform data on {platform["site"]["name"]}')
                mylogs.error(e)

    mylogs.info('Loaded all platforms.')

def graph_create_groups(data):
    # Add user groups function

    def tx_create_groups(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_groups_{chunk}.json") '
            f'YIELD value '
            f'MERGE (g:group {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET g.name = value.name '
            f'WITH g, value '
            f'MATCH (p:platform {{name: "{dataset}"}}) '
            f'WITH g, p '
            f'MERGE (p)<-[:ON_PLATFORM]-(g) '
        )

    def tx_create_group_index(tx):
        tx.run(
            f'CREATE INDEX group IF NOT EXISTS '
            f'FOR (g:group) '
            f'ON (g.discourse_id, g.platform) '
        )

    with driver.session() as session:
        session.write_transaction(tx_create_group_index)
        mylogs.info('Created group index')

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'groups'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.write_transaction(tx_create_groups, str(chunk), platform_name)
                    mylogs.debug(f'Loaded group data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    mylogs.error(f'Import failed for groups on {platform_name}, chunk #{chunk}')
                    mylogs.error(e)

    mylogs.info('Added all groups')

def graph_create_users(data):
    # Add users function

    def tx_create_users(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_users_{chunk}.json") '
            f'YIELD value '
            f'MERGE (u:user {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET u.username = value.username '
            f'SET u.email = value.email '
            f'SET u.consent = value.consent '
            f'SET u.consent_updated = value.consent_updated '
            f'SET u.groups = value.groups '
            f'WITH u, value '
            f'MATCH (p:platform {{name: "{dataset}"}}) '
            f'WITH u, p, value '
            f'MERGE (p)<-[:ON_PLATFORM]-(u) '
            f'SET u.profile = p.url + "/u/" + u.username '
            f'WITH u, value '
            f'UNWIND value.groups AS gids '
            f'MATCH (g:group {{discourse_id: gids, platform: "{dataset}"}}) '
            f'WITH u, g, value '
            f'CREATE (u)-[:IN_GROUP]->(g) '
            f'MERGE (global:globaluser {{email: value.email}}) '
            f'SET global.username = value.username '
            f'WITH global, u '
            f'MERGE (u)-[:IS_GLOBAL_USER]->(global)'
            f'WITH global '
            f'MATCH (p:platform {{name:"{dataset}" }}) '
            f'WITH p, global '
            f'MERGE (p)<-[:HAS_ACCOUNT_ON]-(global)'
        )

    def tx_create_user_index(tx):
        tx.run(
            f'CREATE INDEX user IF NOT EXISTS '
            f'FOR (u:user) '
            f'ON (u.discourse_id, u.platform) '
        )

    def tx_create_globaluser_index(tx):
        tx.run(
            f'CREATE INDEX global IF NOT EXISTS '
            f'FOR (g:globaluser) '
            f'ON (g.email) '
        )

    with driver.session() as session:
        session.write_transaction(tx_create_user_index)
        mylogs.info('Created user index')

    with driver.session() as session:
        session.write_transaction(tx_create_globaluser_index)
        mylogs.info('Created globaluser index')

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'users'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.write_transaction(tx_create_users, chunk, platform_name)
                    mylogs.debug(f'Loaded user data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    mylogs.error(f'Import failed for users on {platform_name}, chunk #{chunk}')
                    mylogs.error(e)

    mylogs.info('Added all users')

def graph_create_tags(data):
    # Add tags function

    def tx_create_tags(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_tags_{chunk}.json") '
            f'YIELD value '
            f'CREATE (tag:tag {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET tag.name = value.name '
            f'SET tag.topic_count = value.topic_count '
            f'SET tag.created_at = value.created_at '
            f'SET tag.updated_at = value.updated_at '
            f'WITH tag, value '
            f'MATCH (p:platform {{name: "{dataset}"}}) '
            f'WITH tag, p, value '
            f'CREATE (p)<-[:ON_PLATFORM]-(tag) '
        )

    def tx_create_tag_index(tx):
        tx.run(
            f'CREATE INDEX tag IF NOT EXISTS '
            f'FOR (t:tag) '
            f'ON (t.discourse_id, t.platform) '
        )

    with driver.session() as session:
        session.write_transaction(tx_create_tag_index)
        mylogs.info('Created tag index')

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'tags'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.write_transaction(tx_create_tags, chunk, platform_name)
                    mylogs.debug(f'Loaded tag data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    mylogs.error(f'Import failed for tag on {platform_name}, chunk #{chunk}')
                    mylogs.error(e)

    mylogs.info('Added all tags')

def graph_create_categories(data):
    # Add categories

    def tx_create_categories(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_categories_{chunk}.json") '
            f'YIELD value '
            f'MERGE (c:category {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET c.name = value.name '
            f'SET c.name_lower = value.name_lower '
            f'SET c.created_at = value.created_at '
            f'SET c.updated_at = value.updated_at '
            f'SET c.read_restricted = value.read_restricted '
            f'SET c.parent_category_id = value.parent_category_id '
            f'SET c.permissions = value.permissions '
            f'WITH c, value '
            f'MATCH (p:platform {{name: "{dataset}"}}) '
            f'CREATE (p)<-[:ON_PLATFORM]-(c) '
            f'WITH c, value '
            f'UNWIND value.permissions AS permissions '
            f'MATCH (g:group {{discourse_id: permissions, platform: "{dataset}"}}) '
            f'MERGE (g)-[:HAS_ACCESS]->(c) '
            f'WITH c, value '
            f'CALL apoc.do.when(value.parent_category_id IS NOT NULL,'
            f'"MERGE (c)<-[:PARENT_CATEGORY_OF]-(ca:category {{discourse_id: value.parent_category_id, platform: dataset}})",'
            f'"",'
            f'{{c:c, value:value, dataset: c.platform}}) '
            f'YIELD value AS value2 '
            f'RETURN value2 '
        )

    def tx_create_category_index(tx):
        tx.run(
            f'CREATE INDEX categories IF NOT EXISTS '
            f'FOR (g:category) '
            f'ON (g.discourse_id, g.platform) '
        )

    with driver.session() as session:
        session.write_transaction(tx_create_category_index)
        mylogs.info('Created category index')

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'categories'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.write_transaction(tx_create_categories, chunk, platform_name)
                    mylogs.debug(f'Loaded category data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    mylogs.error(f'Import failed for categories on {platform_name}, chunk #{chunk}')
                    mylogs.error(e)

    mylogs.info('Added all categories')

def graph_create_topics(data):
    # Add topics

    def tx_create_topics(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_topics_{chunk}.json") '
            f'YIELD value '
            f'CREATE (t:topic {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET t.title = value.title '
            f'SET t.created_at = value.created_at '
            f'SET t.updated_at = value.updated_at '
            f'SET t.user_id = value.user_id '
            f'SET t.is_message_thread = value.is_message_thread '
            f'SET t.tags = value.tags '
            f'SET t.category_id = value.category_id '
            f'WITH t, value '
            f'MATCH (p:platform {{name: "{dataset}"}}) '
            f'CREATE (p)<-[:ON_PLATFORM]-(t) '
            f'WITH t, value '
            f'MATCH (c:category {{discourse_id: value.category_id, platform: "{dataset}"}}) '
            f'CREATE (c)<-[:IN_CATEGORY]-(t) '
            f'WITH t, value '
            f'MATCH (u:user {{discourse_id: value.user_id, platform: "{dataset}"}}) '
            f'CREATE (t)<-[:CREATED]-(u) '
            f'WITH t, value '
            f'UNWIND value.tags AS tagids '
            f'MATCH (tag:tag {{discourse_id: tagids, platform: "{dataset}"}}) '
            f'CREATE (t)-[:TAGGED_WITH]->(tag) '
        )

    def tx_create_topic_index(tx):
        tx.run(
            f'CREATE INDEX topic IF NOT EXISTS '
            f'FOR (t:topic) '
            f'ON (t.discourse_id, t.platform) '
        )

    with driver.session() as session:
        session.write_transaction(tx_create_topic_index)
        mylogs.info('Created topic index')

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'topics'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.write_transaction(tx_create_topics, chunk, platform_name)
                    mylogs.debug(f'Loaded topic data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    mylogs.error(f'Import failed for topic on {platform_name}, chunk #{chunk}')
                    mylogs.error(e)

    mylogs.info('Added all topics')

def graph_create_posts(data):
    # Add posts

    def tx_create_posts(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_posts_{chunk}.json") '
            f'YIELD value '
            f'CREATE (p:post {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET p.user_id = value.user_id '
            f'SET p.topic_id = value.topic_id '
            f'SET p.post_number = value.post_number '
            f'SET p.raw = value.raw '
            f'SET p.created_at = value.created_at '
            f'SET p.updated_at = value.updated_at '
            f'SET p.deleted_at = value.deleted_at '
            f'SET p.hidden = value.hidden '
            f'SET p.word_count = value.word_count '
            f'SET p.wiki = value.wiki '
            f'SET p.reads = value.reads '
            f'SET p.score = value.score '
            f'SET p.like_count = value.like_count '
            f'SET p.reply_count = value.reply_count '
            f'SET p.quote_count = value.quote_count '
            f'WITH p, value '
            f'MATCH (platform:platform {{name: "{dataset}"}}) '
            f'MERGE (platform)<-[:ON_PLATFORM]-(p) '
            f'SET p.topic_url = platform.url + "/t/" + p.topic_id '
            f'SET p.post_url = platform.url + "/t/" + p.topic_id + "/" + p.post_number '
            f'WITH p, value '
            f'MATCH (u:user {{discourse_id: value.user_id, platform: "{dataset}"}}) '
            f'MERGE (p)<-[:CREATED]-(u) '
            f'WITH p, u, value '
            f'MATCH (t:topic {{platform: "{dataset}", discourse_id: value.topic_id}}) '
            f'SET p.username = u.username '
            f'WITH p, t '
            f'SET p.topic_title = t.title '
            f'MERGE (t)<-[r:IN_TOPIC]-(p)'
        )

    def tx_create_post_index(tx):
        tx.run(
            f'CREATE INDEX post IF NOT EXISTS '
            f'FOR (g:post) '
            f'ON (g.discourse_id, g.platform) '
        )

    with driver.session() as session:
        try:
            session.write_transaction(tx_create_post_index)
            mylogs.info('Created post index')
        except Exception as e:
            mylogs.error(f'Creating post index failed')
            mylogs.error(e)

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'posts'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.write_transaction(tx_create_posts, chunk, platform_name)
                    mylogs.debug(f'Loaded post data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    mylogs.error(f'Import failed for posts on {platform_name}, chunk #{chunk}')
                    mylogs.error(e)

    mylogs.info('Added all posts')

def graph_create_replies(data):
    # Add replies
    
    def tx_create_replies(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_replies_{chunk}.json") '
            f'YIELD value '
            f'MATCH (p1:post {{discourse_id: value.reply_post_id, platform: "{dataset}"}}) '
            f'MATCH (p2:post {{discourse_id: value.post_id, platform: "{dataset}"}}) '
            f'CREATE (p2)<-[r:IS_REPLY_TO]-(p1) '
        )
    
    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'replies'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.write_transaction(tx_create_replies, chunk, platform_name)
                    mylogs.debug(f'Loaded reply data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    mylogs.info(f'Import failed for replies on {platform_name}, chunk #{chunk}')
                    mylogs.error(e)

    mylogs.info('Added all reply links')

def graph_create_quotes(data):
    # Add quotes
    
    def tx_create_quotes(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_quotes_{chunk}.json") '
            f'YIELD value '
            f'MATCH (p1:post {{discourse_id: value.quoted_post_id, platform: "{dataset}"}}) '
            f'MATCH (p2:post {{discourse_id: value.post_id, platform: "{dataset}"}}) '
            f'CREATE (p1)<-[r:CONTAINS_QUOTE_FROM]-(p2) '
        )
    
    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'quotes'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.write_transaction(tx_create_quotes, chunk, platform_name)
                    mylogs.debug(f'Loaded quote data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    mylogs.error(f'Import quote for reply on {platform_name}, chunk #{chunk}')
                    mylogs.error(e)

    mylogs.info('Added all quote links')

def graph_create_interactions():
    # Add interactions

    def tx_create_global_user_talks(tx):
        tx.run(
            f'MATCH (g1:globaluser)<-[:IS_GLOBAL_USER]-()-[:CREATED]->()-[r:IS_REPLY_TO]-()<-[:CREATED]-()-[:IS_GLOBAL_USER]->(g2:globaluser) '
            f'WITH g1, g2, count(r) AS c '
            f'MERGE (g1)-[gr:TALKED_TO]-(g2) '
            f'SET gr.count = c '
        )

    def tx_create_user_talks(tx):
        tx.run(
            f'MATCH (u1:user)-[:CREATED]->()-[r:IS_REPLY_TO]-()<-[:CREATED]-(u2:user) '
            f'WITH u1, u2, count(r) AS c '
            f'MERGE (u1)-[ur:TALKED_TO]-(u2) '
            f'SET ur.count = c '
        )

    def tx_create_global_user_quotes(tx):
        tx.run(
            f'MATCH (g1:globaluser)<-[:IS_GLOBAL_USER]-()-[:CREATED]->()-[r:CONTAINS_QUOTE_FROM]->()<-[:CREATED]-()-[:IS_GLOBAL_USER]->(g2:globaluser) '
            f'WITH g1, g2, count(r) AS c '
            f'MERGE (g1)-[gr:QUOTED]->(g2) '
            f'SET gr.count = c '
        )

    def tx_create_user_quotes(tx):
        tx.run(
            f'MATCH (u1:user)-[:CREATED]->()-[r:CONTAINS_QUOTE_FROM]->()<-[:CREATED]-(u2:user) '
            f'WITH u1, u2, count(r) AS c '
            f'MERGE (u1)-[ur:QUOTED]->(u2) '
            f'SET ur.count = c '
        )

    def tx_create_global_user_talks_and_quotes(tx):
        tx.run(
            f'MATCH (g1:globaluser)<-[:IS_GLOBAL_USER]-()-[:CREATED]->(p)-[r:IS_REPLY_TO|CONTAINS_QUOTE_FROM]-()<-[:CREATED]-()-[:IS_GLOBAL_USER]->(g2:globaluser) '
            f'WITH g1, g2, count(DISTINCT p) AS c '
            f'MERGE (g1)-[gr:TALKED_OR_QUOTED]-(g2) '
            f'SET gr.count = c '
        )

    def tx_create_user_talks_and_quotes(tx):
        tx.run(
            f'MATCH (u1:user)-[:CREATED]->(p)-[r:IS_REPLY_TO|CONTAINS_QUOTE_FROM]-()<-[:CREATED]-(u2:user) '
            f'WITH u1, u2, count(DISTINCT p) AS c '
            f'MERGE (u1)-[ur:TALKED_OR_QUOTED]-(u2) '
            f'SET ur.count = c '
        )

    with driver.session() as session:
        try:
            session.write_transaction(tx_create_user_talks)
            mylogs.info('Created user talk graph')
        except Exception as e:
            mylogs.error('Creating user talk graph failed.')
            mylogs.error(e)
        try:
            session.write_transaction(tx_create_global_user_talks)
            mylogs.info('Created global user talk graph')
        except Exception as e:
            mylogs.error('Creating global user talk graph failed.')
            mylogs.error(e)
        try:
            session.write_transaction(tx_create_user_quotes)
            mylogs.info('Created user quote graph')
        except Exception as e:
            mylogs.error('Creating user quote graph failed.')
            mylogs.error(e)
        try:
            session.write_transaction(tx_create_global_user_quotes)
            mylogs.info('Created global user quote graph')
        except Exception as e:
            mylogs.error('Creating global user quote graph failed.')
            mylogs.error(e)
        try:
            session.write_transaction(tx_create_user_talks_and_quotes)
            mylogs.info('Created user talk and quote graph')
        except Exception as e:
            mylogs.error('Creating user talk and quote graph failed.')
            mylogs.error(e)
        try:
            session.write_transaction(tx_create_global_user_talks_and_quotes)
            mylogs.info('Created global user talk and quote graph')
        except Exception as e:
            mylogs.error('Creating global user talk and quote graph failed.')
            mylogs.error(e)

    mylogs.info('Added all user to user interaction links')

def graph_create_likes(data):
    # Add likes

    def tx_create_likes(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_likes_{chunk}.json") '
            f'YIELD value '
            f'MATCH (p:post {{discourse_id: value.post_id, platform: "{dataset}"}}) '
            f'MATCH (u:user {{discourse_id: value.user_id, platform: "{dataset}"}}) '
            f'CREATE (p)<-[r:LIKES]-(u) '
        )

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'likes'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.write_transaction(tx_create_likes, chunk, platform_name)
                    mylogs.debug(f'Loaded likes data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    mylogs.error(f'Import likes for reply on {platform_name}, chunk #{chunk}')
                    mylogs.error(e)

    mylogs.info('Added all like links')

def graph_create_languages(data):
    # Add annotation languages

    def tx_create_create_languages(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_languages_{chunk}.json") '
            f'YIELD value '
            f'CREATE (lang:language {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET lang.name = value.name '
            f'SET lang.locale = value.locale '
            f'WITH lang, value '
            f'MATCH (p:platform {{name: "{dataset}"}}) '
            f'WITH lang, p, value '
            f'MERGE (p)<-[:ON_PLATFORM]-(lang) '
        )

    def tx_create_language_index(tx):
        tx.run(
            f'CREATE INDEX languages IF NOT EXISTS '
            f'FOR (lang:language) '
            f'ON (lang.discourse_id, lang.platform) '
        )

    with driver.session() as session:
        session.write_transaction(tx_create_language_index)
        mylogs.info('Created language index')

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'languages'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.write_transaction(tx_create_create_languages, chunk, platform_name)
                    mylogs.debug(f'Loaded language data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    mylogs.error(f'Import for language on {platform_name}, chunk #{chunk}')
                    mylogs.error(e)

def graph_create_codes(data):
    # Add annotation codes

    def tx_create_codes(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_codes_{chunk}.json") '
            f'YIELD value '
            f'CREATE (code:code {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET code.name = value.name '
            f'SET code.description = value.description '
            f'SET code.creator_id = value.creator_id '
            f'SET code.created_at = value.created_at '
            f'SET code.updated_at = value.updated_at '
            f'SET code.ancestry = value.ancestry '
            f'SET code.annotations_count = value.annotations_count '
            f'WITH code, value '
            f'MATCH (p:platform {{name: "{dataset}"}}) '
            f'WITH code, p, value '
            f'CREATE (p)<-[:ON_PLATFORM]-(code) '
            f'WITH code, value '
            f'MATCH (u:user {{discourse_id: value.creator_id, platform: "{dataset}"}}) '
            f'CREATE (u)-[:CREATED]->(code)'
        )

    def tx_create_code_index(tx):
        tx.run(
            f'CREATE INDEX codes IF NOT EXISTS '
            f'FOR (code:code) '
            f'ON (code.discourse_id, code.platform) '
        )

    with driver.session() as session:
        session.write_transaction(tx_create_code_index)
        mylogs.info('Created code index')

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'codes'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.write_transaction(tx_create_codes, chunk, platform_name)
                    mylogs.debug(f'Loaded code data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    mylogs.error(f'Import for codes on {platform_name}, chunk #{chunk}')
                    mylogs.error(e)

def graph_create_code_ancestry(data):
    # Create ancestry relations

    def tx_create_code_ancestry(tx, dataset):
        tx.run(
            f'MATCH (c:code) WHERE c.ancestry CONTAINS "/" '
            f'WITH toInteger(split(c.ancestry,"/")[-1]) AS cid, c AS child '
            f'MERGE (parent:code {{discourse_id: cid, platform: child.platform}}) '
            f'MERGE (parent)<-[:HAS_PARENT_CODE]-(child) '
        )

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            try:
                session.write_transaction(tx_create_code_ancestry, platform_name)
                mylogs.debug(f'Loaded code ancestry from {platform_name}')
            except Exception as e:
                mylogs.error(f'Import failed for code ancestry on {platform_name}')
                mylogs.error(e)

def graph_create_code_names(data):
    # Add annotation code names

    def tx_create_code_names(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_code_names_{chunk}.json") '
            f'YIELD value '
            f'CREATE (codename:codename {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET codename.name = value.name '
            f'SET codename.code_id = value.code_id '
            f'SET codename.language_id = value.language_id '
            f'SET codename.created_at = value.created_at '
            f'WITH codename, value '
            f'MATCH (language:language {{discourse_id: value.language_id, platform: "{dataset}"}}) '
            f'MATCH (code:code {{discourse_id: value.code_id, platform: "{dataset}"}}) '
            f'WITH codename, language, code '
            f'CREATE (codename)<-[:HAS_CODENAME]-(code) '
            f'CREATE (codename)-[:IN_LANGUAGE]->(language) '
            f'WITH code, codename, language '
            f'CALL apoc.do.when(language.locale = "en",'
            f'"SET code.name = codename.name",'
            f'"",'
            f'{{code:code, codename:codename, language:language}}) '
            f'YIELD value AS value2 '
            f'RETURN value2 '
        )

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'code_names'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.write_transaction(tx_create_code_names, chunk, platform_name)
                    mylogs.debug(f'Loaded code name data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    mylogs.error(f'Import for code name on {platform_name}, chunk #{chunk}')
                    mylogs.error(e)

def graph_create_annotations(data):
    # Add annotations

    def tx_create_annotations(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_annotations_{chunk}.json") '
            f'YIELD value '
            f'CREATE (annotation:annotation {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET annotation.text = value.text '
            f'SET annotation.quote = value.quote '
            f'SET annotation.created_at = value.created_at '
            f'SET annotation.updated_at = value.updated_at '
            f'SET annotation.code_id = value.code_id '
            f'SET annotation.post_id = value.post_id '
            f'SET annotation.creator_id = value.creator_id '
            f'SET annotation.type = value.type '
            f'SET annotation.topic_id = value.topic_id '
            f'WITH annotation, value '
            f'MATCH (code:code {{discourse_id: value.code_id, platform: "{dataset}"}}) '
            f'MATCH (post:post {{discourse_id: value.post_id, platform: "{dataset}"}}) '
            f'MATCH (user:user {{discourse_id: value.creator_id, platform: "{dataset}"}}) '
            f'WITH code, post, user, annotation '
            f'CREATE (code)<-[:REFERS_TO]-(annotation) '
            f'CREATE (post)<-[:ANNOTATES]-(annotation) '
            f'CREATE (user)-[:CREATED]->(annotation) '
            f'SET annotation.creator_username = user.username '
        )

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'annotations'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.write_transaction(tx_create_annotations, chunk, platform_name)
                    mylogs.debug(f'Loaded annotations data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    mylogs.error(f'Import failed for annotations on {platform_name}, chunk #{chunk}')
                    mylogs.error(e)

def graph_create_corpus():
    # Define ethno-tags as corpus identifiers
    # This function expects corpus identifiers to be unique across platforms

    def tx_create_corpus(tx):
        tx.run(
            f'MATCH (t:tag) WHERE t.name STARTS WITH "ethno-" '
            f'SET t:corpus '
            f'WITH t '
            f'MATCH (t)<-[:TAGGED_WITH]-()<-[:IN_TOPIC]-(p:post)<-[:ANNOTATES]-()-[:REFERS_TO]->(code:code) '
            f'WITH code, t '
            f'MERGE (code)-[:IN_CORPUS]->(t)'
        )

    def tx_get_corpora(tx):
        result = tx.run('MATCH (c:corpus) RETURN c.name AS name')
        corpora = []
        for corpus in result:
            corpora.append(corpus['name'])
        return corpora

    def tx_create_corpus_annotation_counts(tx, corpus):
        annotations_variable_prop = corpus.replace('-','_')
        tx.run(
            f'MATCH (corpus:corpus {{name: "{corpus}"}})<-[:TAGGED_WITH]-(t:topic)<-[:IN_TOPIC]-(p:post)<-[:ANNOTATES]-(a:annotation)-[:REFERS_TO]->(code:code) '
            f'WITH code, corpus, count(a) AS corpus_code_use '
            f'MATCH (code)-[r:IN_CORPUS]->(corpus) '
            f'SET r.annotation_count = corpus_code_use '
        )

    with driver.session() as session:
        try:
            session.write_transaction(tx_create_corpus)
            mylogs.info('Added corpus labels to graph')
        except Exception as e:
            mylogs.error('Adding corpus labels to graph failed.')
            mylogs.error(e)

    corpora = []
    with driver.session() as session:
        try:
            corpora = session.read_transaction(tx_get_corpora)
            if corpora:
                for corpus in corpora:
                    session.write_transaction(tx_create_corpus_annotation_counts, corpus)
                mylogs.info('Created annotation counts per corpus')
        except Exception as e:
            mylogs.error('Getting corpora failed.')
            mylogs.error(e)

def graph_create_code_cooccurrences():
    # Create code cooccurance network between codes

    def tx_create_cooccurrence_index(tx):
        tx.run(
            f'CALL db.index.fulltext.createRelationshipIndex("cooccurrenceRelationshipIndex",["COOCCURS"],["count"])'
        )

    def tx_create_code_cooccurrences(tx):
        tx.run(
            f'MATCH (corpus:corpus)<-[:TAGGED_WITH]-()<-[:IN_TOPIC]-(p:post)<-[:ANNOTATES]-()-[:REFERS_TO]->(code1:code)-[:HAS_CODENAME]->(cn1:codename)-[:IN_LANGUAGE]->(l:language {{locale: "en"}}) '
            f'MATCH (corpus:corpus)<-[:TAGGED_WITH]-()<-[:IN_TOPIC]-(p:post)<-[:ANNOTATES]-()-[:REFERS_TO]->(code2:code)-[:HAS_CODENAME]->(cn2:codename)-[:IN_LANGUAGE]->(l:language {{locale: "en"}}) WHERE NOT ID(code1) = ID(code2) '
            f'WITH code1, code2, cn1, cn2, corpus, count(DISTINCT p) AS cooccurs '
            f'MERGE (code1)-[r:COOCCURS {{method: "count", count: cooccurs, corpus: corpus.name}}]-(code2) '
            f'RETURN corpus.name, cn1.name, cn2.name, r.count ORDER BY r.count DESCENDING '
        )

    with driver.session() as session:
        session.write_transaction(tx_create_cooccurrence_index)
        mylogs.info('Created cooccurrence index')

    with driver.session() as session:
        try:
            session.write_transaction(tx_create_code_cooccurrences)
            mylogs.info('Created cooccurance graph')
        except Exception as e:
            mylogs.error('Creating cooccurance graph failed.')
            mylogs.error(e)

def graph_create_code_use():
    # Create code use graph

    def tx_create_code_use(tx):
        tx.run(
            f'MATCH (user)-[r:CREATED]-(:annotation)-[:REFERS_TO]->(code:code) '
            f'WITH user, code, count(r) as use '
            f'MERGE (user)-[:USED_CODE {{count: use}}]->(code) '
        )

    with driver.session() as session:
        try:
            session.write_transaction(tx_create_code_use)
            mylogs.info('Created code use graph')
        except Exception as e:
            mylogs.error('Creating code use graph failed.')
            mylogs.error(e)

# TODO
# Add post permissions with HAS_ACCESS to groups to enable granular graph access

def main():
    databases = config['databases']

    # Load data from Discourse psql databases and dump to json files
    # Data is loaded from JSON files because Neo4j APOC functions are optimized for this.
    dbs = databases[:]
    if config['reload_from_database']:
        reload_data(dbs)
    data = load_data(dbs)

    # Build Neo4j database

    # TODO: Refactor 'for platform in data.values()' loop into function
    # TODO: Refactor create index into function

    mylogs.info(' ')
    mylogs.info('Building Neo4j database...')
    mylogs.info(' ')

    # Calls to update graph 
    graph_clear()
    graph_create_platform(data)
    graph_create_groups(data)
    graph_create_users(data)
    graph_create_tags(data)
    graph_create_categories(data)
    graph_create_topics(data)
    graph_create_posts(data)
    graph_create_replies(data)
    graph_create_quotes(data)
    graph_create_interactions()
    graph_create_likes(data)
    graph_create_languages(data)
    graph_create_codes(data)
    graph_create_code_ancestry(data)
    graph_create_code_names(data)
    graph_create_annotations(data)
    graph_create_corpus()
    graph_create_code_cooccurrences()
    graph_create_code_use()

if __name__ == '__main__':
    main()
