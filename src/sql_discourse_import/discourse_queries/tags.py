def get(log, db_root, db_cursor):
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

    log.info(f'    Got {len(tags.keys())} tags.')
    return tags