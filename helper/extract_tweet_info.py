def decode_hashtags(hashtags):
    # extract the hashtag symbol and turn multiple hashtags into a list
    if hashtags:
        return ', '.join(list(map(lambda x: x["text"], hashtags)))
    else:
        return ""


def extract_tweet_info(dict_data):
    """returns the tweet with extracted information

    Args:
        dict_data: tweeter Json format

    Returns:

    """
    if "extended_tweet" in dict_data.keys():
        # print(dict_data["created_at"],  dict_data["extended_tweet"]["full_text"])
        # TODO: rewrite it into a function to process the tweet text
        tweet_text = dict_data["extended_tweet"]['full_text'].replace('\n', ' ').replace('\r', ' ')
        hashtags = dict_data["extended_tweet"]["entities"]["hashtags"]
    elif "text" in dict_data.keys():
        tweet_text = dict_data["text"].replace('\n', ' ').replace('\r', ' ')
        hashtags = dict_data["entities"]["hashtags"]
    else:
        return None

    retweet = False

    # special handle to retweet
    if "retweeted_status" in dict_data.keys():

        if "extended_tweet" in dict_data["retweeted_status"]:
            # print("this is a retweet and there is 'extended_tweet' file")
            tweet_text = dict_data["retweeted_status"]["extended_tweet"]['full_text'].replace('\n', ' ').replace('\r', ' ')
            hashtags = dict_data["retweeted_status"]["extended_tweet"]["entities"]["hashtags"]
        elif "text" in dict_data["retweeted_status"]:
            tweet_text = dict_data["retweeted_status"]["text"].replace('\n', ' ').replace('\r', ' ')
            hashtags = dict_data["retweeted_status"]["entities"]["hashtags"]
        else:
            return None

        retweet = True

    # decide what information about the tweet to extract for your tweeter project
    message_lst = [tweet_text,
                   decode_hashtags(hashtags),
                   str(dict_data['created_at']),
                   str(dict_data["retweet_count"]),
                   str(dict_data["favorite_count"]),
                   str(retweet),
                   str(dict_data["truncated"]),
                   str(dict_data['id']),
                   str(dict_data['user']['name']),
                   str(dict_data['user']['screen_name']),
                   str(dict_data['user']['followers_count']),
                   str(dict_data['user']['location']),
                   str(dict_data['geo']),
                   '\n'
                   ]

    message = "\t".join(message_lst)

    return message
