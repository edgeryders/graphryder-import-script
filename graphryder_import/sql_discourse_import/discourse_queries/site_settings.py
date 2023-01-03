def get(log, db_root, db_cursor, db_name):

    site_query = f'''
    SELECT 
    value 
    FROM {db_root}site_settings 
    WHERE name = 'vapid_base_url' 
    LIMIT 1 
    '''

    db_cursor.execute(site_query)
    site_data = db_cursor.fetchall()
    site = {
        'name': db_name,
        'url': site_data[0][0]
    }
    log.info(f'    Loading data from {site["url"]} database...')

    return site