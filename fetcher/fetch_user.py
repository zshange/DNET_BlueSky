import requests
from pymongo import MongoClient, errors
from datetime import datetime
import logging
from config import Config
import sys
import os
logger = logging.getLogger(__name__)
from utils import * 
def fetch_users():
    """
    Fetches all Bluesky's PDS  and stores their information in MongoDB.
    Also saves the list of instance names to a file.
    """
    # config = Config(config_path='../config/config.yaml')
    # mongodb_uri = config.get_central_mongodb_uri()
    
    query = {
        "limit":  1
    }
    
    token = config.api.get('central_token')
    if not token:
        logger.error("Central API token is not set in the configuration.")
        raise ValueError("Central API token is not set in the configuration.")
    
    headers = {'Authorization': f'Bearer {token}'}
    logger.info("Sending request to fetch instances list...")
    try:
        response = requests.get("https://instances.social/api/1.0/instances/list", headers=headers, params=query)
        if response.status_code != 200:
            save_error_log(None, "fetch_instances", "API", "Failed to fetch instances", res_code=response.status_code, error_message=response.text)
            logger.error(f"Failed to fetch instances: {response.status_code}")
            raise ConnectionError(f"Failed to fetch instances: {response.status_code}")
        
        data = response.json()
        print(response)
        current_time = datetime.now()
        
        # client = MongoClient(mongodb_uri)
        # db = client['mastodon']
        # instances_collection = db["instances"]
        # # Create unique index on 'name' field
        # create_unique_index(instances_collection, 'name')
        
        # # Prepare documents for insertion
        # instances = data.get("instances", [])
        # client.close()
        
        # Extract all names and save to file
        # names = [item['name'] for item in instances]
        # paths = config.get_paths()
        # instances_list_path = paths.get('instances_list', 'instances_list.txt')
        # with open(instances_list_path, 'w', encoding='utf-8') as name_file:
        #     for name in names:
        #         name_file.write(name + '\n')
        
        # logger.info(f"All instance names have been saved to {instances_list_path}")
    except Exception as e:
        logger.exception(f"An error occurred while fetching instances: {e}")
        raise

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    print(parent_dir)
    if parent_dir not in sys.path:
        sys.path.append(parent_dir)
    # fetch_users()

    
    PDS_HOST = "https://bsky.social"  # 根据您的 PDS 实例调整
    USERNAME = "your_username"
    PASSWORD = "your_password"

    response = requests.post(
        f"{PDS_HOST}/xrpc/com.atproto.server.createSession",
        json={"identifier": USERNAME, "password": PASSWORD},
    )
    response.raise_for_status()
    session = response.json()
    access_token = session["accessJwt"]

