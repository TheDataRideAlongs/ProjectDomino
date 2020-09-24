from facebook_scraper import get_posts
import pandas as pd
from urllib.parse import urlparse

pd.set_option('display.max_colwidth', -1)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class FacebookScrape:

    def __init__(self):
        self.pages = 5
        self.timeout = 5
        self.sleep = None #(int) how long to sleep inbetween requests

    def get_posts_From_Page(self, account: str, pages: int, timeout: int, sleep: int):
        self.pages = pages
        self.timeout = timeout
        self.sleep = sleep
        df = pd.concat([pd.DataFrame([post]) for post in
                        get_posts(account=account, pages=pages, extra_info=True, timeout=timeout, sleep=sleep)],
                       ignore_index=True, sort=False)
        df["page_name"] = account
        df['text'] = df['text'].str.replace(r'\n', '')
        df['post_text'] = df['post_text'].str.replace(r'\n', '')
        df['shared_text'] = df['shared_text'].str.replace(r'\n', '')
        if 'reactions' in df:
            df = self.__normalize_reactions(df)
            urls = self.__parse_urls(df)
            dfs = pd.concat([df, urls], axis=1)
            return dfs
        else:
            urls = self.__parse_urls(df)
            dfs = pd.concat([df, urls], axis=1)
            return dfs

    def get_posts_From_Group(self, group: str, pages: int, timeout: int, sleep: int):
        self.pages = pages
        self.timeout = timeout
        self.sleep = sleep
        df = pd.concat([pd.DataFrame([post]) for post in
                        get_posts(group=group, pages=pages, extra_info=True, timeout=timeout, sleep=sleep)],
                       ignore_index=True, sort=False)
        df["group_name"] = group
        df['text'] = df['text'].str.replace(r'\n', '')
        df['post_text'] = df['post_text'].str.replace(r'\n', '')
        df['shared_text'] = df['shared_text'].str.replace(r'\n', '')
        if 'reactions' in df:
            df = self.__normalize_reactions(df)
            urls = self.__parse_urls(df)
            dfs = pd.concat([df, urls], axis=1)
            return dfs
        else:
            urls = self.__parse_urls(df)
            dfs = pd.concat([df, urls], axis=1)
            return dfs

    def __normalize_reactions(self, df):
        rx = df["reactions"].apply(lambda x: {} if pd.isna(x) else x)
        rx = pd.io.json.json_normalize(rx)
        df.drop('reactions', axis=1, inplace=True)
        return pd.concat([df, rx], axis=1)

    def __parse_urls(self, df):
        url_params = []
        for u in df['link']:
            try:
                parsed = urlparse(u)
                url_params.append({
                    'url': u,
                    'schema': parsed.scheme,
                    'netloc': parsed.netloc,
                    'path': parsed.path,
                    'params': parsed.params,
                    'query': parsed.query,
                    'fragment': parsed.fragment,
                    'username': parsed.username,
                    'password': parsed.password,
                    'hostname': parsed.hostname,
                    'port': parsed.port,
                })
            except Exception as e:
                raise e
            if len(url_params) == 0:
                return pd.DataFrame({
                    'url': pd.Series([], dtype='object'),
                    'schema': pd.Series([], dtype='object'),
                    'netloc': pd.Series([], dtype='object'),
                    'path': pd.Series([], dtype='object'),
                    'params': pd.Series([], dtype='object'),
                    'query': pd.Series([], dtype='object'),
                    'fragment': pd.Series([], dtype='object'),
                    'username': pd.Series([], dtype='object'),
                    'password': pd.Series([], dtype='object'),
                    'hostname': pd.Series([], dtype='object'),
                    'port': pd.Series([], dtype='int64')
                })
        return pd.DataFrame(url_params)





