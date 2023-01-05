def get(log, db_root, db_cursor, categories, users, ensure_consent):
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

    log.info(f'    Got {len(topics.keys())} topics and applied {len(topic_tags_data)} tags.')
    return topics, lost_topics, pm_topic_set, pm_count, topic_tags_data