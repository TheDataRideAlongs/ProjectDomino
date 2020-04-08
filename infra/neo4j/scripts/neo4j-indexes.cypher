CREATE CONSTRAINT tweet_unique_id
ON (n:Tweet) ASSERT n.id IS UNIQUE

CREATE CONSTRAINT account_unique_id
ON (n:Account) ASSERT n.id IS UNIQUE

CREATE CONSTRAINT url_unique_full_url
ON (n:Url) ASSERT n.full_url IS UNIQUE

CREATE INDEX tweet_by_type
FOR (n:Tweet)
ON (n.tweet_type)

CREATE INDEX tweet_by_hydrated
FOR (n:Tweet)
ON (n.hydrated)

CREATE INDEX tweet_by_text
FOR (n:Tweet)
ON (n.text)

CREATE INDEX tweet_by_created_at
FOR (n:Tweet)
ON (n.created_at)

CREATE INDEX tweet_by_record_created_at
FOR (n:Tweet)
ON (n.record_created_at)

CALL db.index.fulltext.createNodeIndex("tweet_by_text_fulltext",["Tweet"],["text"]) 

