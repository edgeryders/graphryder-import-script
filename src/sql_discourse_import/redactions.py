import random
from gibberish import Gibberish

def redact(log, data, pseudonymize_users, omit_protected_content, redact_protected_content, omit_codes_prefix):

    # We accept the redundancy and inefficency of looping through the data
    # and removing records as this makes the code less complicated
    # and makes it easier to add new rules later for how and when to omit data.

    # Omit protected content (posts, categories, groups) from the graph.
    # Content that is not readable by all logged in users is considered protected.
    # This also omits 'hidden' posts, also known as 'whispers'.
    # In the future, we may want to handle 'hidden' posts separately if we 
    # if we want to give access to the graph based on the permissions of a loggen in user.

    omit_system_users = True
    omit_private_messages = True

    # This omits content created by system users and by deleted users.
    # It also omits those users completely from the graph.

    users = data['users']
    new = dict(users)
    pseudonyms = []
    gib = Gibberish()
    if pseudonymize_users:
        pseudonym_list = gib.generate_words(max(users.keys()) + 1)
    for u, d in users.items():
        if omit_system_users and d['id'] < 0:
            del(new[u])
            continue
        if pseudonymize_users:
            name = pseudonym_list[u]
            while name in pseudonyms:
                name = name + '_' + random.choice(pseudonym_list)
            pseudonyms.append(name)
            new[u]['username'] = name
    data['users'] = new

    new = dict(data['groups'])
    # Group visibility levels, public=0, logged_on_users=1, members=2, staff=3, owners=4
    for g, d in data['groups'].items():
        if redact_protected_content and d['visibility_level'] > 1:
            new[g]['name'] = '[Redacted]'
            continue
        if omit_protected_content and d['visibility_level'] > 1:
            del(new[g])
            continue
    data['groups'] = new

    new = dict(data['categories'])
    for c, d in data['categories'].items():
        if redact_protected_content and d['read_restricted']:
            new[c]['name'] = '[Redacted]'
            continue
        if omit_protected_content and d['read_restricted']:
            del(new[c])
            continue
        for group in d['permissions']:
            if group not in data['groups'].keys():
                new[c]['permissions'].remove(group)
    data['categories'] = new

    new = dict(data['topics'])
    for t, d in data['topics'].items():
        if omit_private_messages and d['is_message_thread']:
            del(new[t])
            continue
        if redact_protected_content and d['read_restricted']:
            new[t]['title'] = '[Redacted]'
            continue
        if omit_protected_content and d['read_restricted']:
            del(new[t])
            continue
    data['topics'] = new

    new = dict(data['posts'])
    pm_post_set = set()
    for p, d in data['posts'].items():
        if omit_private_messages and d['is_private']:
            pm_post_set.add(d['id'])
            del(new[p])
            continue
        if omit_protected_content and d['hidden']:
            del(new[p])
            continue
        if redact_protected_content and d['read_restricted']:
            new[p]['raw'] = '[Redacted]'
        if omit_protected_content and (d['read_restricted'] or d['hidden']):
            del(new[p])
            continue
    data['posts'] = new

    new = dict(data['quotes'])
    for q, d in data['quotes'].items():
        if omit_private_messages and (d['quoted_post_id'] in pm_post_set or d['post_id'] in pm_post_set):
            del(new[q])
            continue
        if omit_protected_content and (d['quoted_post_id'] in pm_post_set or d['post_id'] not in data['posts'].keys()):
            del(new[q])
            continue
    data['quotes'] = new
    
    new = dict(data['likes'])
    for l, d in data['likes'].items():
        if omit_private_messages and d['post_id'] in pm_post_set:
            del(new[l])
            continue
        if omit_protected_content and d['post_id'] not in data['posts'].keys():
            del(new[l])
            continue
    data['likes'] = new

    new = dict(data['code_names'])
    omitted = set()
    for a, d in data['code_names'].items():
        if omit_codes_prefix:
            for prefix in omit_codes_prefix:
                if d['name'].startswith(prefix):
                    omitted.add(d['code_id'])
                    if d['code_id'] in data['codes'].keys():
                        del[data['codes'][d['code_id']]]
                    del[new[a]]
                    continue

    new = dict(data['annotations'])
    for a, d in data['annotations'].items():
        if d['code_id'] in omitted:
            del(new[a])
            continue
        if omit_private_messages and d['post_id'] in pm_post_set:
            del(new[a])
            continue
        if redact_protected_content and d['post_id'] not in data['posts'].keys():
            new[a]['quote'] = '[Redacted]' 
        if omit_protected_content and d['post_id'] not in data['posts'].keys():
            del(new[a])
            continue
    data['annotations'] = new

    if omit_private_messages:
        log.info('Omitted private messages.')
    if omit_protected_content:
        log.info('Omitted protected content.')
    if omit_system_users:
        log.info('Omitted system users and content.')
    
    return data