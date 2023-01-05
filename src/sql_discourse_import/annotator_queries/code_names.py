def get(log, db_root, db_cursor):
    
    annotator_code_names_query = f'''
    SELECT
    id, name, code_id, language_id, created_at
    FROM {db_root}discourse_annotator_code_names
    '''

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

    log.info(f'    Got {len(list(annotator_code_names.keys()))} code names.')
    
    return annotator_code_names
