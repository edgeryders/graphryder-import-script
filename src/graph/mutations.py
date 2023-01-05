import os

data_path = os.path.abspath('./db/')

def graph_clear(log, driver):
    # Clear database function

    def tx_clear_neo4j(tx):
        tx.run(
            f'call apoc.periodic.iterate("MATCH (n) '
            f'return id(n) as id", "MATCH (n) WHERE id(n) = id DETACH DELETE n", '
            f'{{batchSize:10000}}) '   
            f'yield batches, total return batches, total '         
            )

    def tx_clear_fullTextIndexes(tx):
        tx.run(
            f'CALL db.index.fulltext.drop("cooccurrenceRelationshipIndex") '
            )

    with driver.session() as session:
        try:
            session.execute_write(tx_clear_neo4j)
            session.execute_write(tx_clear_fullTextIndexes)
            log.info('Cleared database')
        except Exception as e:
            log.error('Clearing database failed')
            log.error(e)

def graph_create_platform(log, driver, data):
    # Add platforms function

    def tx_create_platform(tx, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_site.json")'
            f'YIELD value '
            f'CREATE (p:platform {{url: value.url, name: "{dataset}"}})'
        )

    def tx_create_platform_index(tx):
        tx.run(
            f'CREATE INDEX platform IF NOT EXISTS '
            f'FOR (p:platform) '
            f'ON (p.name) '
        )

    with driver.session() as session:
        session.execute_write(tx_create_platform_index)

    for platform in data.values():
        with driver.session() as session:
            try:
                session.execute_write(tx_create_platform, platform['site']['name'])
                log.debug(f'Loaded platform data from {platform["site"]["name"]}')
            except Exception as e:
                log.error(f'Import failed for platform data on {platform["site"]["name"]}')
                log.error(e)

    log.info('Loaded all platforms.')

def graph_create_groups(log, driver, data):
    # Add user groups function

    def tx_create_groups(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_groups_{chunk}.json") '
            f'YIELD value '
            f'MERGE (g:group {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET g.name = value.name '
            f'WITH g, value '
            f'MATCH (p:platform {{name: "{dataset}"}}) '
            f'WITH g, p '
            f'MERGE (p)<-[:ON_PLATFORM]-(g) '
        )

    def tx_create_group_index(tx):
        tx.run(
            f'CREATE INDEX group IF NOT EXISTS '
            f'FOR (g:group) '
            f'ON (g.discourse_id, g.platform) '
        )

    with driver.session() as session:
        session.execute_write(tx_create_group_index)
        log.info('Created group index')

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'groups'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.execute_write(tx_create_groups, str(chunk), platform_name)
                    log.debug(f'Loaded group data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    log.error(f'Import failed for groups on {platform_name}, chunk #{chunk}')
                    log.error(e)

    log.info('Added all groups')

def graph_create_users(log, driver, data):
    # Add users function

    def tx_create_users(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_users_{chunk}.json") '
            f'YIELD value '
            f'MERGE (u:user {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET u.username = value.username '
            f'SET u.email = value.email '
            f'SET u.consent = value.consent '
            f'SET u.consent_updated = value.consent_updated '
            f'SET u.groups = value.groups '
            f'WITH u, value '
            f'MATCH (p:platform {{name: "{dataset}"}}) '
            f'WITH u, p, value '
            f'MERGE (p)<-[:ON_PLATFORM]-(u) '
            f'SET u.profile = p.url + "/u/" + u.username '
            f'WITH u, value '
            f'UNWIND value.groups AS gids '
            f'MATCH (g:group {{discourse_id: gids, platform: "{dataset}"}}) '
            f'WITH u, g, value '
            f'CREATE (u)-[:IN_GROUP]->(g) '
            f'MERGE (global:globaluser {{email: value.email}}) '
            f'SET global.username = value.username '
            f'WITH global, u '
            f'MERGE (u)-[:IS_GLOBAL_USER]->(global)'
            f'WITH global '
            f'MATCH (p:platform {{name:"{dataset}" }}) '
            f'WITH p, global '
            f'MERGE (p)<-[:HAS_ACCOUNT_ON]-(global)'
        )

    def tx_create_user_index(tx):
        tx.run(
            f'CREATE INDEX user IF NOT EXISTS '
            f'FOR (u:user) '
            f'ON (u.discourse_id, u.platform) '
        )

    def tx_create_globaluser_index(tx):
        tx.run(
            f'CREATE INDEX global IF NOT EXISTS '
            f'FOR (g:globaluser) '
            f'ON (g.email) '
        )

    with driver.session() as session:
        session.execute_write(tx_create_user_index)
        log.info('Created user index')

    with driver.session() as session:
        session.execute_write(tx_create_globaluser_index)
        log.info('Created globaluser index')

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'users'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.execute_write(tx_create_users, chunk, platform_name)
                    log.debug(f'Loaded user data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    log.error(f'Import failed for users on {platform_name}, chunk #{chunk}')
                    log.error(e)

    log.info('Added all users')

def graph_create_tags(log, driver, data):
    # Add tags function

    def tx_create_tags(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_tags_{chunk}.json") '
            f'YIELD value '
            f'CREATE (tag:tag {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET tag.name = value.name '
            f'SET tag.topic_count = value.topic_count '
            f'SET tag.created_at = value.created_at '
            f'SET tag.updated_at = value.updated_at '
            f'WITH tag, value '
            f'MATCH (p:platform {{name: "{dataset}"}}) '
            f'WITH tag, p, value '
            f'CREATE (p)<-[:ON_PLATFORM]-(tag) '
        )

    def tx_create_tag_index(tx):
        tx.run(
            f'CREATE INDEX tag IF NOT EXISTS '
            f'FOR (t:tag) '
            f'ON (t.discourse_id, t.platform) '
        )

    with driver.session() as session:
        session.execute_write(tx_create_tag_index)
        log.info('Created tag index')

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'tags'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.execute_write(tx_create_tags, chunk, platform_name)
                    log.debug(f'Loaded tag data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    log.error(f'Import failed for tag on {platform_name}, chunk #{chunk}')
                    log.error(e)

    log.info('Added all tags')

def graph_create_categories(log, driver, data):
    # Add categories

    def tx_create_categories(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_categories_{chunk}.json") '
            f'YIELD value '
            f'MERGE (c:category {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET c.name = value.name '
            f'SET c.name_lower = value.name_lower '
            f'SET c.created_at = value.created_at '
            f'SET c.updated_at = value.updated_at '
            f'SET c.read_restricted = value.read_restricted '
            f'SET c.parent_category_id = value.parent_category_id '
            f'SET c.permissions = value.permissions '
            f'WITH c, value '
            f'MATCH (p:platform {{name: "{dataset}"}}) '
            f'CREATE (p)<-[:ON_PLATFORM]-(c) '
            f'WITH c, value '
            f'UNWIND value.permissions AS permissions '
            f'MATCH (g:group {{discourse_id: permissions, platform: "{dataset}"}}) '
            f'MERGE (g)-[:HAS_ACCESS]->(c) '
            f'WITH c, value '
            f'CALL apoc.do.when(value.parent_category_id IS NOT NULL,'
            f'"MERGE (c)<-[:PARENT_CATEGORY_OF]-(ca:category {{discourse_id: value.parent_category_id, platform: dataset}})",'
            f'"",'
            f'{{c:c, value:value, dataset: c.platform}}) '
            f'YIELD value AS value2 '
            f'RETURN value2 '
        )

    def tx_create_category_index(tx):
        tx.run(
            f'CREATE INDEX categories IF NOT EXISTS '
            f'FOR (g:category) '
            f'ON (g.discourse_id, g.platform) '
        )

    with driver.session() as session:
        session.execute_write(tx_create_category_index)
        log.info('Created category index')

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'categories'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.execute_write(tx_create_categories, chunk, platform_name)
                    log.debug(f'Loaded category data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    log.error(f'Import failed for categories on {platform_name}, chunk #{chunk}')
                    log.error(e)

    log.info('Added all categories')

def graph_create_topics(log, driver, data):
    # Add topics

    def tx_create_topics(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_topics_{chunk}.json") '
            f'YIELD value '
            f'CREATE (t:topic {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET t.title = value.title '
            f'SET t.created_at = value.created_at '
            f'SET t.updated_at = value.updated_at '
            f'SET t.user_id = value.user_id '
            f'SET t.is_message_thread = value.is_message_thread '
            f'SET t.tags = value.tags '
            f'SET t.category_id = value.category_id '
            f'WITH t, value '
            f'MATCH (p:platform {{name: "{dataset}"}}) '
            f'CREATE (p)<-[:ON_PLATFORM]-(t) '
            f'WITH t, value '
            f'MATCH (c:category {{discourse_id: value.category_id, platform: "{dataset}"}}) '
            f'CREATE (c)<-[:IN_CATEGORY]-(t) '
            f'WITH t, value '
            f'MATCH (u:user {{discourse_id: value.user_id, platform: "{dataset}"}}) '
            f'CREATE (t)<-[:CREATED]-(u) '
            f'WITH t, value '
            f'UNWIND value.tags AS tagids '
            f'MATCH (tag:tag {{discourse_id: tagids, platform: "{dataset}"}}) '
            f'CREATE (t)-[:TAGGED_WITH]->(tag) '
        )

    def tx_create_topic_index(tx):
        tx.run(
            f'CREATE INDEX topic IF NOT EXISTS '
            f'FOR (t:topic) '
            f'ON (t.discourse_id, t.platform) '
        )

    with driver.session() as session:
        session.execute_write(tx_create_topic_index)
        log.info('Created topic index')

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'topics'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.execute_write(tx_create_topics, chunk, platform_name)
                    log.debug(f'Loaded topic data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    log.error(f'Import failed for topic on {platform_name}, chunk #{chunk}')
                    log.error(e)

    log.info('Added all topics')

def graph_create_posts(log, driver, data):
    # Add posts

    def tx_create_posts(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_posts_{chunk}.json") '
            f'YIELD value '
            f'CREATE (p:post {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET p.user_id = value.user_id '
            f'SET p.topic_id = value.topic_id '
            f'SET p.post_number = value.post_number '
            f'SET p.raw = value.raw '
            f'SET p.created_at = value.created_at '
            f'SET p.updated_at = value.updated_at '
            f'SET p.deleted_at = value.deleted_at '
            f'SET p.hidden = value.hidden '
            f'SET p.word_count = value.word_count '
            f'SET p.wiki = value.wiki '
            f'SET p.reads = value.reads '
            f'SET p.score = value.score '
            f'SET p.like_count = value.like_count '
            f'SET p.reply_count = value.reply_count '
            f'SET p.quote_count = value.quote_count '
            f'WITH p, value '
            f'MATCH (platform:platform {{name: "{dataset}"}}) '
            f'MERGE (platform)<-[:ON_PLATFORM]-(p) '
            f'SET p.topic_url = platform.url + "/t/" + p.topic_id '
            f'SET p.post_url = platform.url + "/t/" + p.topic_id + "/" + p.post_number '
            f'WITH p, value '
            f'MATCH (u:user {{discourse_id: value.user_id, platform: "{dataset}"}}) '
            f'MERGE (p)<-[:CREATED]-(u) '
            f'WITH p, u, value '
            f'MATCH (t:topic {{platform: "{dataset}", discourse_id: value.topic_id}}) '
            f'SET p.username = u.username '
            f'WITH p, t '
            f'SET p.topic_title = t.title '
            f'MERGE (t)<-[r:IN_TOPIC]-(p)'
        )

    def tx_create_post_index(tx):
        tx.run(
            f'CREATE INDEX post IF NOT EXISTS '
            f'FOR (g:post) '
            f'ON (g.discourse_id, g.platform) '
        )

    with driver.session() as session:
        try:
            session.execute_write(tx_create_post_index)
            log.info('Created post index')
        except Exception as e:
            log.error(f'Creating post index failed')
            log.error(e)

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'posts'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.execute_write(tx_create_posts, chunk, platform_name)
                    log.debug(f'Loaded post data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    log.error(f'Import failed for posts on {platform_name}, chunk #{chunk}')
                    log.error(e)

    log.info('Added all posts')

def graph_create_replies(log, driver, data):
    # Add replies
    
    def tx_create_replies(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_replies_{chunk}.json") '
            f'YIELD value '
            f'MATCH (p1:post {{discourse_id: value.reply_post_id, platform: "{dataset}"}}) '
            f'MATCH (p2:post {{discourse_id: value.post_id, platform: "{dataset}"}}) '
            f'CREATE (p2)<-[r:IS_REPLY_TO]-(p1) '
        )
    
    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'replies'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.execute_write(tx_create_replies, chunk, platform_name)
                    log.debug(f'Loaded reply data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    log.info(f'Import failed for replies on {platform_name}, chunk #{chunk}')
                    log.error(e)

    log.info('Added all reply links')

def graph_create_quotes(log, driver, data):
    # Add quotes
    
    def tx_create_quotes(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_quotes_{chunk}.json") '
            f'YIELD value '
            f'MATCH (p1:post {{discourse_id: value.quoted_post_id, platform: "{dataset}"}}) '
            f'MATCH (p2:post {{discourse_id: value.post_id, platform: "{dataset}"}}) '
            f'CREATE (p1)<-[r:CONTAINS_QUOTE_FROM]-(p2) '
        )
    
    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'quotes'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.execute_write(tx_create_quotes, chunk, platform_name)
                    log.debug(f'Loaded quote data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    log.error(f'Import quote for reply on {platform_name}, chunk #{chunk}')
                    log.error(e)

    log.info('Added all quote links')

def graph_create_interactions(log, driver):
    # Add interactions

    def tx_create_global_user_talks(tx):
        tx.run(
            f'MATCH (g1:globaluser)<-[:IS_GLOBAL_USER]-()-[:CREATED]->()-[r:IS_REPLY_TO]-()<-[:CREATED]-()-[:IS_GLOBAL_USER]->(g2:globaluser) '
            f'WITH g1, g2, count(r) AS c '
            f'MERGE (g1)-[gr:TALKED_TO]-(g2) '
            f'SET gr.count = c '
        )

    def tx_create_user_talks(tx):
        tx.run(
            f'MATCH (u1:user)-[:CREATED]->()-[r:IS_REPLY_TO]-()<-[:CREATED]-(u2:user) '
            f'WITH u1, u2, count(r) AS c '
            f'MERGE (u1)-[ur:TALKED_TO]-(u2) '
            f'SET ur.count = c '
        )

    def tx_create_global_user_quotes(tx):
        tx.run(
            f'MATCH (g1:globaluser)<-[:IS_GLOBAL_USER]-()-[:CREATED]->()-[r:CONTAINS_QUOTE_FROM]->()<-[:CREATED]-()-[:IS_GLOBAL_USER]->(g2:globaluser) '
            f'WITH g1, g2, count(r) AS c '
            f'MERGE (g1)-[gr:QUOTED]->(g2) '
            f'SET gr.count = c '
        )

    def tx_create_user_quotes(tx):
        tx.run(
            f'MATCH (u1:user)-[:CREATED]->()-[r:CONTAINS_QUOTE_FROM]->()<-[:CREATED]-(u2:user) '
            f'WITH u1, u2, count(r) AS c '
            f'MERGE (u1)-[ur:QUOTED]->(u2) '
            f'SET ur.count = c '
        )

    def tx_create_global_user_talks_and_quotes(tx):
        tx.run(
            f'MATCH (g1:globaluser)<-[:IS_GLOBAL_USER]-()-[:CREATED]->(p)-[r:IS_REPLY_TO|CONTAINS_QUOTE_FROM]-()<-[:CREATED]-()-[:IS_GLOBAL_USER]->(g2:globaluser) '
            f'WITH g1, g2, count(DISTINCT p) AS c '
            f'MERGE (g1)-[gr:TALKED_OR_QUOTED]-(g2) '
            f'SET gr.count = c '
        )

    def tx_create_user_talks_and_quotes(tx):
        tx.run(
            f'MATCH (u1:user)-[:CREATED]->(p)-[r:IS_REPLY_TO|CONTAINS_QUOTE_FROM]-()<-[:CREATED]-(u2:user) '
            f'WITH u1, u2, count(DISTINCT p) AS c '
            f'MERGE (u1)-[ur:TALKED_OR_QUOTED]-(u2) '
            f'SET ur.count = c '
        )

    with driver.session() as session:
        try:
            session.execute_write(tx_create_user_talks)
            log.info('Created user talk graph')
        except Exception as e:
            log.error('Creating user talk graph failed.')
            log.error(e)
        try:
            session.execute_write(tx_create_global_user_talks)
            log.info('Created global user talk graph')
        except Exception as e:
            log.error('Creating global user talk graph failed.')
            log.error(e)
        try:
            session.execute_write(tx_create_user_quotes)
            log.info('Created user quote graph')
        except Exception as e:
            log.error('Creating user quote graph failed.')
            log.error(e)
        try:
            session.execute_write(tx_create_global_user_quotes)
            log.info('Created global user quote graph')
        except Exception as e:
            log.error('Creating global user quote graph failed.')
            log.error(e)
        try:
            session.execute_write(tx_create_user_talks_and_quotes)
            log.info('Created user talk and quote graph')
        except Exception as e:
            log.error('Creating user talk and quote graph failed.')
            log.error(e)
        try:
            session.execute_write(tx_create_global_user_talks_and_quotes)
            log.info('Created global user talk and quote graph')
        except Exception as e:
            log.error('Creating global user talk and quote graph failed.')
            log.error(e)

    log.info('Added all user to user interaction links')

def graph_create_likes(log, driver, data):
    # Add likes

    def tx_create_likes(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_likes_{chunk}.json") '
            f'YIELD value '
            f'MATCH (p:post {{discourse_id: value.post_id, platform: "{dataset}"}}) '
            f'MATCH (u:user {{discourse_id: value.user_id, platform: "{dataset}"}}) '
            f'CREATE (p)<-[r:LIKES]-(u) '
        )

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'likes'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.execute_write(tx_create_likes, chunk, platform_name)
                    log.debug(f'Loaded likes data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    log.error(f'Import likes for reply on {platform_name}, chunk #{chunk}')
                    log.error(e)

    log.info('Added all like links')

def graph_create_languages(log, driver, data):
    # Add annotation languages

    def tx_create_languages(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_languages_{chunk}.json") '
            f'YIELD value '
            f'CREATE (lang:language {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET lang.name = value.name '
            f'SET lang.locale = value.locale '
            f'WITH lang, value '
            f'MATCH (p:platform {{name: "{dataset}"}}) '
            f'WITH lang, p, value '
            f'MERGE (p)<-[:ON_PLATFORM]-(lang) '
        )

    def tx_create_language_index(tx):
        tx.run(
            f'CREATE INDEX languages IF NOT EXISTS '
            f'FOR (lang:language) '
            f'ON (lang.discourse_id, lang.platform) '
        )

    with driver.session() as session:
        session.execute_write(tx_create_language_index)
        log.info('Created language index')

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'languages'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.execute_write(tx_create_languages, chunk, platform_name)
                    log.debug(f'Loaded language data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    log.error(f'Import for language on {platform_name}, chunk #{chunk}')
                    log.error(e)

def graph_create_projects(log, driver, data):
    # Add annotation projects

    def tx_create_projects(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_projects_{chunk}.json") '
            f'YIELD value '
            f'CREATE (proj:project {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET proj.name = value.name '
            f'WITH proj, value '
            f'MATCH (p:platform {{name: "{dataset}"}}) '
            f'WITH proj, p, value '
            f'MERGE (p)<-[:ON_PLATFORM]-(proj) '
        )

    def tx_create_project_index(tx):
        tx.run(
            f'CREATE INDEX projects IF NOT EXISTS '
            f'FOR (proj:project) '
            f'ON (proj.discourse_id, proj.platform) '
        )
    
    with driver.session() as session:
        session.execute_write(tx_create_project_index)
        log.info('Created project index')
    
    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'languages'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.execute_write(tx_create_projects, chunk, platform_name)
                    log.debug(f'Loaded project data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    log.error(f'Import error for project on {platform_name}, chunk #{chunk}')
                    log.error(e)

def graph_create_codes(log, driver, data):
    # Add annotation codes

    def tx_create_codes(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_codes_{chunk}.json") '
            f'YIELD value '
            f'CREATE (code:code {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET code.name = value.name '
            f'SET code.description = value.description '
            f'SET code.creator_id = value.creator_id '
            f'SET code.created_at = value.created_at '
            f'SET code.updated_at = value.updated_at '
            f'SET code.ancestry = value.ancestry '
            f'SET code.annotations_count = value.annotations_count '
            f'SET code.project = value.project '
            f'WITH code, value '
            f'MATCH (p:platform {{name: "{dataset}"}}) '
            f'WITH code, p, value '
            f'CREATE (p)<-[:ON_PLATFORM]-(code) '
            f'WITH code, value '
            f'MATCH (proj:project {{discourse_id: value.project}}) '
            f'WITH proj, code, value '
            f'CREATE (proj)<-[:IN_PROJECT {{annotation_count: code.annotations_count}}]-(code) '
            f'WITH code, value '
            f'MATCH (u:user {{discourse_id: value.creator_id, platform: "{dataset}"}}) '
            f'CREATE (u)-[:CREATED]->(code)'
        )

    def tx_create_code_index(tx):
        tx.run(
            f'CREATE INDEX codes IF NOT EXISTS '
            f'FOR (code:code) '
            f'ON (code.discourse_id, code.platform) '
        )

    with driver.session() as session:
        session.execute_write(tx_create_code_index)
        log.info('Created code index')

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'codes'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.execute_write(tx_create_codes, chunk, platform_name)
                    log.debug(f'Loaded code data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    log.error(f'Import for codes on {platform_name}, chunk #{chunk}')
                    log.error(e)

def graph_create_code_ancestry(log, driver, data):
    # Create ancestry relations

    def tx_create_code_ancestry(tx, dataset):
        tx.run(
            f'MATCH (c:code) WHERE c.ancestry CONTAINS "/" '
            f'WITH toInteger(split(c.ancestry,"/")[-1]) AS cid, c AS child '
            f'MERGE (parent:code {{discourse_id: cid, platform: child.platform}}) '
            f'MERGE (parent)<-[:HAS_PARENT_CODE]-(child) '
        )

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            try:
                session.execute_write(tx_create_code_ancestry, platform_name)
                log.debug(f'Loaded code ancestry from {platform_name}')
            except Exception as e:
                log.error(f'Import failed for code ancestry on {platform_name}')
                log.error(e)

def graph_create_code_names(log, driver, data):
    # Add annotation code names

    def tx_create_code_names(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_code_names_{chunk}.json") '
            f'YIELD value '
            f'CREATE (codename:codename {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET codename.name = value.name '
            f'SET codename.code_id = value.code_id '
            f'SET codename.language_id = value.language_id '
            f'SET codename.created_at = value.created_at '
            f'WITH codename, value '
            f'MATCH (language:language {{discourse_id: value.language_id, platform: "{dataset}"}}) '
            f'MATCH (code:code {{discourse_id: value.code_id, platform: "{dataset}"}}) '
            f'WITH codename, language, code '
            f'CREATE (codename)<-[:HAS_CODENAME]-(code) '
            f'CREATE (codename)-[:IN_LANGUAGE]->(language) '
            f'WITH code, codename, language '
            f'CALL apoc.do.when(language.locale = "en",'
            f'"SET code.name = codename.name",'
            f'"",'
            f'{{code:code, codename:codename, language:language}}) '
            f'YIELD value AS value2 '
            f'RETURN value2 '
        )

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'code_names'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.execute_write(tx_create_code_names, chunk, platform_name)
                    log.debug(f'Loaded code name data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    log.error(f'Import for code name on {platform_name}, chunk #{chunk}')
                    log.error(e)

def graph_create_annotations(log, driver, data):
    # Add annotations

    def tx_create_annotations(tx, chunk, dataset):
        tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_annotations_{chunk}.json") '
            f'YIELD value '
            f'CREATE (annotation:annotation {{discourse_id: value.id, platform: "{dataset}"}}) '
            f'SET annotation.text = value.text '
            f'SET annotation.quote = value.quote '
            f'SET annotation.created_at = value.created_at '
            f'SET annotation.updated_at = value.updated_at '
            f'SET annotation.code_id = value.code_id '
            f'SET annotation.post_id = value.post_id '
            f'SET annotation.creator_id = value.creator_id '
            f'SET annotation.type = value.type '
            f'SET annotation.topic_id = value.topic_id '
            f'WITH annotation, value '
            f'MATCH (code:code {{discourse_id: value.code_id, platform: "{dataset}"}}) '
            f'MATCH (post:post {{discourse_id: value.post_id, platform: "{dataset}"}}) '
            f'MATCH (user:user {{discourse_id: value.creator_id, platform: "{dataset}"}}) '
            f'WITH code, post, user, annotation '
            f'CREATE (code)<-[:REFERS_TO]-(annotation) '
            f'CREATE (post)<-[:ANNOTATES]-(annotation) '
            f'CREATE (user)-[:CREATED]->(annotation) '
            f'SET annotation.creator_username = user.username '
        )

    for platform in data.values():
        with driver.session() as session:
            platform_name = platform['site']['name']
            topic = 'annotations'
            chunks = platform['stats']['chunk_sizes'][topic]
            for chunk in range(1, chunks + 1):
                try:
                    session.execute_write(tx_create_annotations, chunk, platform_name)
                    log.debug(f'Loaded annotations data from {platform_name}, chunk #{chunk}')
                except Exception as e:
                    log.error(f'Import failed for annotations on {platform_name}, chunk #{chunk}')
                    log.error(e)

def graph_create_code_cooccurrences(log, driver):
    # Create code cooccurance network between codes

    def tx_create_cooccurrence_index(tx):
        tx.run(
            f'CALL db.index.fulltext.createRelationshipIndex("cooccurrenceRelationshipIndex",["COOCCURS"],["count"])'
        )

    def tx_create_code_cooccurrences(tx):
        tx.run(
            f'MATCH (project:project)<-[:IN_PROJECT]-(code1:code)-[:HAS_CODENAME]->(cn1:codename)-[:IN_LANGUAGE]->(l:language {{locale: "en"}}) '
            f'WITH project, code1, cn1 '
            f'MATCH (code1)<-[:REFERS_TO]-()-[:ANNOTATES]->(p:post)<-[:ANNOTATES]-()-[:REFERS_TO]->(code2)-[:IN_PROJECT]->(project) WHERE NOT ID(code1) = ID(code2) '
            f'WITH project, code1, code2, cn1, p '
            f'MATCH (code2)-[:HAS_CODENAME]->(cn2:codename)-[:IN_LANGUAGE]->(l:language {{locale: "en"}}) '
            f'WITH project, code1, code2, cn1, cn2, count(DISTINCT p) AS cooccurs  '
            f'MERGE (code1)-[r:COOCCURS {{method: "count", count: cooccurs, project: project.name}}]-(code2) '
            f'RETURN project.name, cn1.name, cn2.name, r.count ORDER BY r.count DESCENDING '
        )

    with driver.session() as session:
        session.execute_write(tx_create_cooccurrence_index)
        log.info('Created cooccurrence index')

    with driver.session() as session:
        try:
            session.execute_write(tx_create_code_cooccurrences)
            log.info('Created cooccurance graph')
        except Exception as e:
            log.error('Creating cooccurance graph failed.')
            log.error(e)

def graph_create_code_use(log, driver):
    # Create code use graph

    def tx_create_code_use(tx):
        tx.run(
            f'MATCH (user)-[r:CREATED]-(:annotation)-[:REFERS_TO]->(code:code) '
            f'WITH user, code, count(r) as use '
            f'MERGE (user)-[:USED_CODE {{count: use}}]->(code) '
        )

    with driver.session() as session:
        try:
            session.execute_write(tx_create_code_use)
            log.info('Created code use graph')
        except Exception as e:
            log.error('Creating code use graph failed.')
            log.error(e)
