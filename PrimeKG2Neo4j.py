from configparser import ConfigParser
from neo4j import GraphDatabase
import pandas as pd

def connect_neo4j():
    config = ConfigParser()
    config.read('config')

    username = config.get('Neo4j', 'username')
    password = config.get('Neo4j', 'password')
    host = config.get('Neo4j', 'host')

    driver = GraphDatabase.driver(host, auth=(username, password))

    return driver

def excute_query(query, parameters=''):
    with driver.session() as session:
        return session.run(query, parameters)


def insert_sample_data(driver):
    name = 'Tom Hanks'
    query = f'''CREATE (p:Person {{name: $name, born: 1956}})
                CREATE (m:Movie {{title: 'Forrest Gump', released: 1994}})
                CREATE (p)-[r:ACTED_IN {{roles: ['Forrest']}}]->(m)
                RETURN p, m, r'''
    parameters = { 'name': name }
    excute_query(query, parameters)


def insert_data(driver):
    df = pd.read_csv('Alzheimer.csv')
    # df = pd.read_csv('kg.csv')

    arr = df[['x_type', 'y_type', 'relation']].values
    unique_pairs = set(tuple(pair) for pair in arr)
    len(unique_pairs)

    for types in unique_pairs:

        mask = (df['x_type'] == types[0]) & (df['y_type'] == types[1]) & (df['relation'] == types[2])
        temp_df = df[mask]
        temp_df = temp_df.replace('/', '_', regex=True)
        
        replaced_types = tuple(s.replace('/', '_') for s in types)
        records = temp_df.to_dict('records')

        query = f'''
        UNWIND $records AS record
        MERGE (x:{replaced_types[0]} {{id: record.x_id, name: record.x_name, type: record.x_type, source: record.x_source}})
        MERGE (y:{replaced_types[1]} {{id: record.y_id, name: record.y_name, type: record.y_type, source: record.y_source}})
        MERGE (x)-[r:{replaced_types[2]} {{relation: record.relation, display_relation: record.display_relation}}]->(y)
        '''
    
        excute_query(query, parameters={'records': records})


def empty_database(driver):
    query = '''MATCH (n)
               DETACH DELETE n'''
    excute_query(query)

def list_record(driver):
    query = '''match (n) return (n)'''
    with driver.session() as session:
        records = session.run(query)

        for record in records:
            print(record.keys, record.values)

driver = connect_neo4j()

empty_database(driver)
# insert_sample_data(driver)
insert_data(driver)

# list_record(driver)
