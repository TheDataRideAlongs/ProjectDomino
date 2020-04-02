import pandas as pd
import logging
from modules.Neo4jDataAccess import Neo4jDataAccess

df = pd.DataFrame({'id': [1219746154621165568,
                          1219746203652456448,
                          1219746235038474243,
                          1219746508955967488,
                          1219746544955453441]
                   })
# DEBUG, INFO, WARNING, ERROR, CRITICAL
logging.getLogger().setLevel(logging.WARNING)
pd.set_option('display.max_columns', 500)
pd.set_option('display.max_colwidth', None)


# Test save_parquet_df_to_graph
print('----')
print('Testing Parquet Save')
tdf = pd.read_parquet(
    './data/2020_03_22_02_b1.snappy2.parquet', engine='pyarrow')
Neo4jDataAccess().save_parquet_df_to_graph(tdf, 'dave')
print('----')

# Test get_tweet_hydrated_status_by_id
print('----')
print('Testing get_tweet_hydrated_status_by_id')
df = Neo4jDataAccess().get_tweet_hydrated_status_by_id(df)
print(df)

# Test get_tweet_by_id
print('----')
print('Testing get_tweet_by_id')
df = Neo4jDataAccess().get_tweet_by_id(df)
print(df)
print('----')
print('Testing get_tweet_by_id for cols')
df = Neo4jDataAccess().get_tweet_by_id(
    df, cols=['id', 'job_name', 'created_at'])
print('Column check: ' + df.columns)
print('----')

# Test get_from_neo
print('----')
print('Testing get_from_neo for Limit 1')
df = Neo4jDataAccess().get_from_neo(
    'MATCH (n:Tweet) WHERE n.hydrated=\'FULL\' RETURN n.id, n.text LIMIT 5')
print(df)
print('----')

print('----')
print('Testing get_from_neo for Limit 1')
df = Neo4jDataAccess().get_from_neo(
    'MATCH (n:Tweet) WHERE n.hydrated=\'FULL\' RETURN n.id, n.text LIMIT 5', limit=1)
print(df)
print('----')

print('----')
print('Testing get_from_neo with limit only')
df = Neo4jDataAccess().get_from_neo(
    'MATCH (n:Tweet) RETURN n', limit=1)
print(df)
print('----')

print('Done')
