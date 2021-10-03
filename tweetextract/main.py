import json
import os

import boto3
import pandas as pd
import requests
from requests import status_codes
from yaml import dump, safe_load

# import black


def load_credentials():

    with open("tweetextract/keyconfig.yaml", "r") as credential:

        yamlLoaded = safe_load(credential)
        bearer_token = yamlLoaded.get("BEARER_TOKEN")
    return bearer_token


# commment here about the fuction and its output


def extract(bearer_token):
    """[summary]paginate through an API response and update output json file

    Args:
        bearer_token ([type]): [Oath2.0 authentication]
    """
    next = None
    query_params = {
        "query": "#TigrayGenocide",
        "tweet.fields": "created_at,geo,author_id,in_reply_to_user_id,possibly_sensitive,public_metrics,referenced_tweets,source",
        "expansions": "author_id,geo.place_id",
        "place.fields": "country,country_code,full_name,geo,id,name,place_type",
        "user.fields": "description,id,location,name,public_metrics,username,verified",
    }
    query_params_withNext = {
        "query": "#TigrayGenocide",
        "tweet.fields": "created_at,geo,author_id,in_reply_to_user_id,possibly_sensitive,public_metrics,referenced_tweets,source",
        "expansions": "author_id,geo.place_id",
        "place.fields": "country,country_code,full_name,geo,id,name,place_type",
        "user.fields": "description,id,location,name,public_metrics,username,verified",
        "next_token": next,
    }

    url = "https://api.twitter.com/2/tweets/search/recent"
    header = {"Authorization": "Bearer {}".format(bearer_token)}
    tweet_count = 0
    response_count = 0

    while response_count <= 5:

        if next == None:

            r = requests.request("GET", url, headers=header, params=query_params).json()
        else:
            r = requests.request(
                "GET", url, headers=header, params=query_params_withNext
            ).json()

        if response_count == 0:

            with open(
                "/home/meron/Desktop/devfile/tweetExtract/tweetextract/outputjson.json",
                "w",
            ) as f:

                json.dump(r, f)
            with open(
                "/home/meron/Desktop/devfile/tweetExtract/tweetextract/outputjson.json",
                "r",
            ) as f:
                test = json.load(f)
                if test["meta"]["next_token"] != None:
                    next = test["meta"]["next_token"]
        elif response_count == 1:

            with open(
                "/home/meron/Desktop/devfile/tweetExtract/tweetextract/outputjson1.json",
                "w",
            ) as f:
                json.dump(r, f)

            base_path = "/home/meron/Desktop/devfile/tweetExtract/tweetextract/"
            file1 = os.path.join(base_path, "outputjson.json")
            file2 = os.path.join(base_path, "outputjson1.json")
            next = json_con2(file1, file2)
        else:
            with open(file2, "w") as f:
                json.dump(r, f)
            json_con2(file1, file2)
        response_count += 1
        print(response_count)


def json_con2(json1, json2):
    """[concatinates json responses into a sinle json file]

    Args:
        json1 ([Str]): [json1 path]
        json2 ([Str]): [json2 path]

    Returns:
        [type]: [description] returns
    the next token of the request response
    """
    with open(json1) as file1:
        data1 = json.load(file1)
    with open(json2) as file2:
        data2 = json.load(file2)
    data1["data"] = data1["data"] + data2["data"]
    try:
        data1["includes"] = data1["includes"]["users"] + data2["includes"]["users"]
    except:
        pass
    else:
        data1["includes"] = data1.get("includes") + data2.get("includes")["users"]

    data1["meta"]["result_count"] = (
        data1["meta"]["result_count"] + data2["meta"]["result_count"]
    )
    data1["meta"]["next_token"] = data2["meta"]["next_token"]
    next = data1["meta"]["next_token"]
    with open("tweetextract/outputjson.json", "w") as f:
        json.dump(data1, f)
    return next

    # NOTE: deploy to heroku worker app (which will run evertime and request twitter api again
    # or use twitter pager
    # )


def s3_connect(stream):
    s3_client = boto3.client("s3")
    s3_client.put_object(Body="stream", Bucket="tweetstaging", Key="data")


# TODO: dump tweet data to aws s3 as they come (stream it):
# issue:s3 might not like overwriting data so find a way to by pass that

# TODO: write a function to process the json file and write it to postgres database


def main():
    c = load_credentials()
    # {testing formatter                }

    jsonout = extract(c)

    # s3_connect(jsonout)


# TODO: automate the pipeline

if __name__ == "__main__":

    main()
