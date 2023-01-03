import hashlib
def get(log, db_root, db_cursor, salt, config):

    users_query = f'''
    SELECT
    users.id, username_lower, email
    FROM {db_root}users AS users, {db_root}user_emails as emails
    WHERE users.id = emails.user_id;
    '''
    users = {}
    db_cursor.execute(users_query)
    users_data = db_cursor.fetchall()
    # If emails are redacted, we use a salted hash to link user accounts together
    # Since we need the same email to return the same hash, we use one salt for all platforms and users. 
    if config['redact_emails']:
        log.info(f'    Redacting emails and replacing with hashes...')
    for user in users_data:
        uid = user[0]
        email = user[2]
        hashed_email = hashlib.pbkdf2_hmac(
            'sha256',
            email.encode('utf-8'),
            salt,
            1000,
            dklen=128
        )
        if config['redact_emails']:
            email = hashed_email
        users[uid] = {
            'id': uid,
            'username': user[1],
            'email': email,
            'groups': [],
            'consent': 0,
            'consent_updated': 0
        }
    
    # Adding another system user as a dummy user for private and deleted content
    users[-100] = {
        'id': -100,
        'username': "Unknown",
        'email': "Unknown",
        'groups': [],
        'consent': 0,
        'consent_updated': 0
    }

    consent_query = f'''
    SELECT 
    user_id, value, updated_at 
    FROM {db_root}user_custom_fields 
    WHERE name = 'edgeryders_consent';
    '''

    db_cursor.execute(consent_query)
    consent_data = db_cursor.fetchall()
    for user in consent_data:
        uid = user[0]
        if user[1] == '1':
            users[uid]['consent'] = 1
        else:
            users[uid]['consent'] = 0
        users[uid]['consent_updated'] = user[2]

    group_members_query = f'''
    SELECT
    group_id, user_id
    FROM {db_root}group_users
    '''

    db_cursor.execute(group_members_query)
    group_members_data = db_cursor.fetchall()
    for group_member in group_members_data:
        uid = group_member[1]
        users[uid]['groups'].append(group_member[0])

    log.info(f'    Got {len(users.keys())} users')

    return users