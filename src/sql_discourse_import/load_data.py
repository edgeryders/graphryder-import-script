import json

def load_from_files(log, dbs):

    log.info('')
    log.info('Loading JSON data files to verify...')
    log.info('')
    data = {}
    for db in dbs:
        data[db['name']] = {}
        with open(f'./db/{db["name"]}_site.json') as file:
            data[db['name']]['site'] = json.load(file)
        with open(f'./db/{db["name"]}_stats.json') as file:
            data[db['name']]['stats'] = json.load(file)
            stats = data[db['name']]['stats']

        for topic, chunks in stats['chunk_sizes'].items():
            data[db['name']][topic] = []
            for chunk in range(1, chunks + 1):
                with open(f'./db/{db["name"]}_{topic}_{chunk}.json') as file:
                    data[db['name']][topic].extend(json.load(file))

    for k,d in data.items():
        log.info(f'-------| {k} |-------')
        log.info(f'{len(d["users"])} users')
        log.info(f'{len(d["groups"])} groups')
        log.info(f'{len(d["tags"])} tags.')
        log.info(f'{len(d["categories"])} tags.')
        log.info(f'{len(d["topics"])} topics and {d["stats"]["pm_threads"]} PM threads.')
        log.info(f'{d["stats"]["tags_applied"]} tag applications to topics.')
        log.info(f'{len(d["posts"])} posts and {d["stats"]["messages"]} private messages.')
        log.info(f'{len(d["replies"])} posts are replies to other posts.')
        log.info(f'{len(d["quotes"])} quotes.')
        log.info(f'{len(d["likes"])} likes.')
        
        if d['stats']['omit_pm']:
            log.info('Private messages have been omitted.')
        else:
            log.info('Private message content and message user identification has been omitted from the dataset.')

        if d['stats']['omit_protected']:
            log.info('Protected content has been omitted.')

        if d['stats']['omit_system_users']:
            log.info('System users and their content has been omitted.')

        log.info(f'Annotation languages:{d["stats"]["annotator_languages"]}.') 
        log.info(f'Annotation projects:{d["stats"]["annotator_projects"]}.')
        log.info(f'{len(d["codes"])} ethnographic codes with {len(d["code_names"])} names.')
        log.info(f'{len(d["annotations"])} ethnographic annotations.')
        log.info(' ')

    return data