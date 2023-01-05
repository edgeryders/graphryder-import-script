def get(log, db_root, db_cursor):
    
    annotator_codes_query = f'''
    SELECT
    id, description, creator_id, created_at, updated_at, ancestry, annotations_count, project_id
    FROM {db_root}discourse_annotator_codes
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
            'annotations_count': code[6],
            'project': code[7]
        }

    log.info(f'    Got {len(list(annotator_codes.keys()))} codes.')
    
    return annotator_codes
