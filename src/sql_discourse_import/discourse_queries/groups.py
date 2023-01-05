def get(log, db_root, db_cursor):

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

    log.info(f'    Got {len(groups.keys())} groups')

    return groups