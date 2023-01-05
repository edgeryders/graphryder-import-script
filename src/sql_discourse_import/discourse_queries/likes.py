def get(log, db_root, db_cursor, posts):

    likes_query = f'''
    SELECT
    post_id, user_id
    FROM {db_root}post_actions
    WHERE post_action_type_id = 2
    '''

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

    log.info(f'    Got {len(likes.keys())} likes.')
    return likes