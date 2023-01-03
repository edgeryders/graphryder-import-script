def get(log, db_root, db_cursor, posts):

    quotes_query = f'''
    SELECT
    post_id, quoted_post_id
    FROM {db_root}quoted_posts
    '''

    quotes = {}
    db_cursor.execute(quotes_query)
    quotes_data = db_cursor.fetchall()
    for num, quote in enumerate(quotes_data):
        posts[quote[0]]['quotes_posts'].append(quote[1])
        quotes[num] = {
            'id': num,
            'post_id': quote[0],
            'quoted_post_id': quote[1]
        }

    log.info(f'    Got {len(quotes.keys())} quotes.')
    return quotes