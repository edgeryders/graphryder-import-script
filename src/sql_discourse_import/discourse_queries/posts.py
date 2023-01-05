def get(log, db_root, db_cursor, topics, users, ensure_consent):
    # Get posts
    # Private messages are excluded

    posts_query = f'''
    SELECT
    id, user_id, topic_id, post_number, raw, created_at, updated_at, deleted_at, hidden, word_count, wiki, reads, score, like_count, reply_count, quote_count
    FROM {db_root}posts
    '''

    allowed_users_query = f'''
    SELECT 
    topic_id, user_id
    FROM {db_root}topic_allowed_users
    '''
    pm_count = 0
    pm_topic_set = set()
    db_cursor.execute(allowed_users_query)
    allowed_users_data = db_cursor.fetchall()
    for permission in allowed_users_data:
        tid = permission[0]
        pm_topic_set.add(tid)
        pm_count += 1

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

    log.info(f'    Got {len(posts.keys())} posts.')

    return posts, private_count, pm_topic_set