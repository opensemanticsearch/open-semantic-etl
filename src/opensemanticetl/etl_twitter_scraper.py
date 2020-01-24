#!/usr/bin/python3
# -*- coding: utf-8 -*-

import datetime
import time
import twint
import sys
from etl import ETL
from tasks import index_web

module = sys.modules["twint.storage.write"]

etl = ETL()
etl.read_configfile('/etc/opensemanticsearch/etl')
etl.read_configfile('/etc/opensemanticsearch/etl-webadmin')

etl.config['plugins'] = ['enhance_path', 'enhance_entity_linking', 'enhance_multilingual']
etl.config['facet_path_strip_prefix'] = ["http://www.", "https://www.", "http://", "https://"]


def index_tweet(obj, config):
    tweet = obj.__dict__

    parameters = {}
    parameters['id'] = tweet['link']

    data = {}
    data['content_type_ss'] = 'Tweet'
    data['content_type_group_ss'] = 'Social media post'

    data['author_ss'] = tweet['name']
    data['userid_s'] = tweet['user_id_str']
    data['username_ss'] = tweet['username']

    data['title_txt'] = tweet['tweet']
    data['content_txt'] = tweet['tweet']

    data['hashtag_ss'] = tweet['hashtags']

    if tweet['place']:
        data['location_ss'] = tweet['place']

    data['urls_ss'] = tweet['urls']

    data['mentions_ss'] = tweet['mentions']

    data['retweets_count_i'] = tweet['retweets_count']
    data['likes_count_i'] = tweet['likes_count']
    data['replies_count_i'] = tweet['replies_count']
    data['file_modified_dt'] = tweet['datestamp'] + 'T' + tweet['timestamp'] + 'Z'

    if config.Index_Linked_Webpages:
        if data['urls_ss']:
            for url in data['urls_ss']:
                index_web.apply_async(kwargs={'uri': url}, queue='tasks', priority=5)

    try:
        etl.process(parameters, data)
    except BaseException as e:
        sys.stderr.write("Exception while indexing tweet {} : {}".format(parameters['id'], e))

# overwrite twint json export method with custom function index_tweet
module.Json = index_tweet


def index(search=None, username=None, Profile_full=False, limit=None, Index_Linked_Webpages=False):

    c = twint.Config()
    c.Hide_output = True
    c.Store_json = True
    c.Output = "tweets.json"

    if username:
        c.Username = username

    if search:
        c.Search = search

    if limit:
        c.Limit = limit

    c.Index_Linked_Webpages = Index_Linked_Webpages

    c.Profile_full = Profile_full

    if Profile_full:
        twint.run.Profile(c)
    else:
        twint.run.Search(c)

    etl.commit()



#
# If running from command line (not imported as library) get parameters and start
#

if __name__ == "__main__":

    # get uri or filename from args

    from optparse import OptionParser

    # get uri or filename from args

    parser = OptionParser("etl-twitter-scraper [options]")
    parser.add_option("-u", "--user", dest="username",
                      default=None, help="User")
    parser.add_option("-s", "--search", dest="search",
                      default=None, help="Search")
    parser.add_option("-l", "--limit", dest="limit",
                      default=None, help="Limit")

    (options, args) = parser.parse_args()

    if not options.username and not options.search:
        parser.error("No Username or search given")

    index(username=options.username, search=options.search, limit=options.limit)


