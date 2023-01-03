def get(log, db_root, db_cursor):

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

    log.info(f'    Got {len(categories.keys())} categories')
    return categories
