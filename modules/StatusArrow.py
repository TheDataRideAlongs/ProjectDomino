import pyarrow as pa

### When dtype -> arrow ambiguious, override
KNOWN_FIELDS = [
    [0, 'contributors', pa.string()],
    [1, 'coordinates', pa.string()],
    [2, 'created_at', pa.string()],

     #[3, 'display_text_range', pa.list_(pa.int64())],
    [3, 'display_text_range', pa.string()],

    [4, 'entities', pa.string()],
    [5, 'extended_entities', pa.string()], #extended_entities_t ],
    [7, 'favorited', pa.bool_()],
    [8, 'favorite_count', pa.int64()],
    [9, 'full_text', pa.string()],
    [10, 'geo', pa.string()],
    [11, 'id', pa.int64() ],
    [12, 'id_str', pa.string() ],
    [13, 'in_reply_to_screen_name', pa.string() ],
    [14, 'in_reply_to_status_id', pa.int64() ],
    [15, 'in_reply_to_status_id_str', pa.string() ],
    [16, 'in_reply_to_user_id', pa.int64() ],
    [17, 'in_reply_to_user_id_str', pa.string() ],
    [18, 'is_quote_status', pa.bool_() ],
    [19, 'lang', pa.string() ],
    [20, 'place', pa.string()],
    [21, 'possibly_sensitive', pa.bool_()],
    [22, 'quoted_status', pa.string()],
    [23, 'quoted_status_id', pa.int64()],
    [24, 'quoted_status_id_str', pa.string()],
    [25, 'quoted_status_permalink', pa.string()],
    [26, 'retweet_count', pa.int64()],
    [27, 'retweeted', pa.bool_()],
    [28, 'retweeted_status', pa.string()],
    [29, 'scopes', pa.string()],
    [30, 'source', pa.string()],
    [31, 'truncated', pa.bool_()],
    [32, 'user', pa.string()],

    #[33, 'withheld_in_countries', pa.list_(pa.string())],
    [33, 'withheld_in_countries', pa.string()],

    #[34, 'followers', pa.struct({'followers': pa.bool_()})]
    [34, 'followers', pa.string()]
]



