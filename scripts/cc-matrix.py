import os
import json
import logging
import numpy as np
from neo4j import GraphDatabase

mylogs = logging.getLogger(__name__)
mylogs.setLevel(logging.DEBUG)

with open("./config.json") as json_config:
    config = json.load(json_config)

uri = config['neo4j_uri']
driver = GraphDatabase.driver(uri, auth=(config['neo4j_user'], config['neo4j_password']))
data_path = os.path.abspath('./db/')

# Save data in chunks of size n
def dumpSplit(data_topic, data_set, data, stats={'chunk_sizes': {}}):
    path = './db/'
    n = 1000
    data_chunks = [data[i * n:(i + 1) * n] for i in range((len(data) + n - 1) // n )]
    for num, item in enumerate(data_chunks):
        with open(f'{path}{data_set}_{data_topic}_{str(num+1)}.json', 'w') as file:
            json.dump(item, file, default=str)
    stats['chunk_sizes'][data_topic] = len(data_chunks)
    return stats

def graph_create_association_depth():
    # Update cooccurance relations with association depth if specified for corpus in conf 

    def tx_get_post_annotations(tx, corpus):
        result = tx.run(
            f'MATCH (posts:post)-[:IN_TOPIC]->(t:topic)-[:TAGGED_WITH]->(c:corpus {{name: "{corpus}"}}) '
            f'WITH posts '
            f'MATCH (posts)<-[:ANNOTATES]-(a:annotation)-[:REFERS_TO]->(codes:code) '
            f'RETURN posts.discourse_id AS post, codes.discourse_id AS code, count(a) AS annotation_count '
        )
        annotations = {}
        for record in result:
            post_id = record["post"]
            code_id = record["code"]
            annotation_count = record["annotation_count"]
            
            if post_id not in annotations.keys():
                annotations[post_id] = {}

            annotations[post_id][code_id] = annotation_count
        
        return annotations

    def tx_create_multiplied_code_cooccurrence(tx, chunk, platform, corpus):
        dataset = platform + '-' + corpus
        result = tx.run(
            f'CALL apoc.load.json("file://{data_path}/{dataset}_assocations_depth_{chunk}.json") '
            f'YIELD value '
            # f'MERGE (code1:code {{discourse_id: value.code1, platform: "{platform}" }})-[r:COOCCURS {{corpus: "{corpus}", count: value.weight, method: "association-depth"}}]-(code2:code {{discourse_id: value.code2, platform: "{platform}" }}) '
            # Alternative method to create new relation
            f'MATCH (code1:code {{discourse_id: value.code1, platform: "{platform}" }}) '
            f'MATCH (code2:code {{discourse_id: value.code2, platform: "{platform}" }}) '
            # f'RETURN value, code1.name, code2.name '
            f'CREATE (code1)-[r:COOCCURS {{corpus: "{corpus}", count: value.weight, method: "association-depth"}}]->(code2) '
            f'RETURN value, code1.name, code2.name, r.count'
        )

    corpora = []
    platforms = config['databases']
    for platform in platforms:
        for corpus in platform['association_depth']:
            corpora.append([platform['name'], corpus])

    with driver.session() as session:

        for c in corpora:
            platform = c[0]
            corpus = c[1]

            try:
                post_annotations = session.read_transaction(tx_get_post_annotations, corpus)
                codes = set()
                for c in post_annotations.values():
                    for code in c.keys():
                        codes.add(code)
                codes = list(codes)

                cc_matrix = np.zeros((max(codes)+1,max(codes)+1))

                for c in post_annotations.values():
                    for code1 in c.keys():
                        for code2 in c.keys():
                            if code1 is not code2:
                                cc_matrix[code1, code2] += c[code1]*c[code2]

                cc_matrix = np.tril(cc_matrix)

                association_relations = []
                for code1 in codes:
                    for code2 in codes:
                        weight = cc_matrix[code1, code2]
                        if weight > 0:
                            association_relations.append({'code1': code1, 'code2': code2, 'platform': platform, 'weight': int(weight)})
                
                chunks = dumpSplit('assocations_depth', platform + '-' + corpus, association_relations)['chunk_sizes']['assocations_depth']

                for chunk in range(1, chunks + 1):
                    try:
                        session.write_transaction(tx_create_multiplied_code_cooccurrence, chunk, platform, corpus)
                        mylogs.debug(f'Loaded association depth data from {platform}, chunk #{chunk}')
                    except Exception as e:
                        mylogs.error(f'Import failed for association depth on {platform}, chunk #{chunk}')
                        mylogs.error(e)
                           
                mylogs.info('Created association depth graph')
            except Exception as e:
                mylogs.error('Created association depth graph.')
                mylogs.error(e)

graph_create_association_depth()