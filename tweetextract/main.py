import pandas as pd
from requests import status_codes 
from yaml import safe_load,dump 
import requests
import json
import boto3



def load_credentials():
    with open('tweetextract/keyconfig.yaml','r') as credential:

        yamlLoaded=safe_load(credential)
        bearer_token=yamlLoaded.get('BEARER_TOKEN')
    return bearer_token

def extract(bearer_token):
    query_params = {'query': '#TigrayGenocide',
    'tweet.fields':'created_at,geo,author_id,in_reply_to_user_id,possibly_sensitive,public_metrics,referenced_tweets,source',
    'expansions':'author_id,geo.place_id',
    'place.fields':'country,country_code,full_name,geo,id,name,place_type',
    'user.fields':'description,id,location,name,public_metrics,username,verified'
    }
    #tweet.fields=author_id,context_annotations,created_at,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,source,text,
    #withheld&expansions=referenced_tweets.id'
    url="https://api.twitter.com/2/tweets/search/recent"
    header={'Authorization': 'Bearer {}'.format(bearer_token)}
    r = requests.request('GET',url,headers=header,params=query_params)
    
    with open('/home/meron/Desktop/devfile/tweetExtract/tweetextract/outputjson.json', 'w') as jsonout:
            json.dump(r.json(),jsonout)
 #TODO: looping through the twitter api results in overwriting the json response..find solution   
    if r.status_code==200:
        """ with open('/home/meron/Desktop/devfile/tweetExtract/tweetextract/outputjson.json', 'r') as jsonout:
            dic=json.load(jsonout)
        dic.update(r.json())
        with open('/home/meron/Desktop/devfile/tweetExtract/tweetextract/outputjson.json', 'w') as jsonout:
            json.dump(dic,jsonout)
        #print(len(jsonout)) """
    return jsonout


def s3_connect(stream):
    s3_client=boto3.client('s3')
    s3_client.put_object(Body='stream',Bucket='tweetstaging',Key='data')
    

#TODO: dump tweet data to aws s3 as they come (stream it):
                #issue:s3 might not like overwriting data so find a way to by pass that

#TODO: write a function to process the json file and write it to postgres database

    

def main():
    c=load_credentials()
    jsonout=extract(c)
    s3_connect(jsonout)

#TODO: automate the pipeline
if __name__=="__main__":

    main()
