CREATE INDEX tweet_by_tweet_id FOR (n:Tweet) ON (n.tweet_id);
CREATE INDEX account_by_user_id FOR (n:Account) ON (n.user_id);