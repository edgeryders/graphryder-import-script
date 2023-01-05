def get(log, db_root, db_cursor, ensure_consent, posts):
    
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

    log.info(f'    Got {len(list(annotations.keys()))} annotations.')
    
    return annotations
