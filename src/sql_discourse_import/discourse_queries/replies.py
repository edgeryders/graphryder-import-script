def get(log, db_root, db_cursor, posts):

    replies_query = f'''
    SELECT
    post_id, reply_post_id
    FROM {db_root}post_replies
    '''

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

    log.info(f'    Got {len(replies.keys())} replies.')
    return replies