import os
import json
import random
import requests
import mysql.connector
import threading
from time import sleep
from dotenv import load_dotenv
#Load environment variables from .env file
load_dotenv()
db_host = os.getenv('DB_HOST')
db_port = int(os.getenv('DB_PORT'))
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
database = os.getenv('DATABASE')
proxy = os.getenv('PROXY')
sleep_time = int(os.getenv('SLEEP_TIME'))
input_table = os.getenv('INPUT_TABLE')
output_table = os.getenv('OUTPUT_TABLE')
max_following = int(os.getenv('MAX_FOLLOWING'))
accounts_number = int(os.getenv('ACCOUNTS_NUMBER'))
print(f'\n db_host={db_host} \n db_port={db_port} \n db_user={db_user} \n db_password={db_password} \n database={database} \n\n proxy={proxy} \n sleep_time={sleep_time} \n input_table={input_table} \n output_table={output_table} \n max_following={max_following} \n accounts_number={accounts_number}\n')
 
def get_userids_from_DB(input_table, accounts_number, default=5):
    connection = mysql.connector.connect(host=db_host, port=db_port, user=db_user, password=db_password, database=database)
    cursor = connection.cursor()
    query = f'select user_id from {input_table} where is_processed nor in ('Done') limit {accounts_number};'
    print(f'\nFetching users from source table, Query= {query}')
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    user_ids = [ userid[0] for userid in result ]
    print(f'\tUsers returned from source table: {user_ids}\n')
    return user_ids
 
def insert_followings_data(followings, user_id, output_table):
    print(f'\t\t\t\t{user_id}: Starting function "insert_followings_data"')
    connection = mysql.connector.connect(host=db_host, port=db_port, user=db_user, password=db_password, database=database)
    cursor = connection.cursor()
    query = f'INSERT INTO {output_table}(user_id,username,title,isprivate,isverified,followedby) VALUES(%s, %s, %s, %s, %s, %s)'  
    print(f'\t\t\t\t{user_id}: inserting followings data into target table, query: {query}')
    cursor.executemany(query, followings)
    connection.commit()
    cursor.close()
    print(f'\t\t\t\t{user_id}: Function insert_followings_data completed')
 
def update_isProcessed_accounts2(input_table, user_id, is_processed):
    print(f'\t\t\t{user_id}: Starting function "update_isProcessed_accounts2", is_processed: {is_processed}')
    connection = mysql.connector.connect(host=db_host, port=db_port, user=db_user, password=db_password, database=database)
    cursor = connection.cursor()
    query = f'update {input_table} set is_processed={is_processed} where user_id={user_id}'
    cursor.execute(query)
    connection.commit()
    cursor.close()
    print(f'\t\t\t{user_id}: Completed function "update_isProcessed_accounts2", is_processed: {is_processed}')
 
def get_followings(output_table, user_id, max_following):
    print(f'\nStarting "get_followings" thread for user_id: {user_id}')
    print(f'\t{user_id}: Max followings to fetch: {max_following}')
    followings = []
    next_max_id = 200
    while next_max_id != 'Last' and next_max_id <= max_following:
        print(f'\n\t{user_id} : Entered while loop')
        headers = {
            "Accept": "*/*",
            "cookie": random.choice(sessions),
            "x-ig-app-id": "<instagram-app-id>",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
            "Connection": "keep-alive",
            "Accept-Encoding": "gzip, deflate, br",
            "proxy-connection":proxy
        }
        print(f'\t{user_id} headers : {headers["cookie"]} ')
        url = f'https://i.instagram.com/api/v1/friendships/{user_id}/following/?count=200&max_id={next_max_id}'
        print(f'\t{user_id} url : {url}')
        response = requests.request("GET", url, headers=headers)
        try:
            print(f'\t\t{user_id}: Response.status_code: {response.status_code}')
            print(f'\t\t{user_id}: Response.text.status: {json.loads(response.text)["status"]}')
            if response.status_code == 200 and json.loads(response.text)['status'] == 'ok':
                data = json.loads(response.text)
                #print(response.text)
                next_max_id = int(data.get('next_max_id', 'Last'))
                print(f'\t\t\t\t{user_id} next_max_id: {next_max_id}')
                for user in data['users']:
                    #print(f'\t\t\t\t Entered For Loop')
                    followings_user_id = user['pk_id']
                    username = user['username']
                    full_name = user['full_name']
                    is_private = user['is_private']
                    is_verified = user['is_verified']
                    followings.append(tuple([followings_user_id, username, full_name, is_private, is_verified, user_id]))                
                #print(f'Followings: {followings}')
                print(f'\t\t\t\t{user_id}: calling function insert_followings_data')
                insert_followings_data(followings, user_id, output_table)
                is_processed = 'Done'
                print(f'\t\t\t\t{user_id} is_processed: {is_processed}')
        except:
            print(f'\t\t{user_id}: entered except block')
            is_processed = 'Failed'
            next_max_id = 'Last'
        finally:
            print(f'\t\t{user_id}: entered finally block')
            #call function to update isProcessed column in accounts_2 Table for user
            update_isProcessed_accounts2(input_table, user_id, is_processed)
    print(f'Completed "get_followings" thread for user_id: {user_id}\n')

 
while True:
    #fetch user_ids from DB
    user_ids = get_userids_from_DB(input_table, accounts_number)
    if len(user_ids) == 0:
        sleep(sleep_time)
    else:
        thread_list = []
        for user_id in user_ids:
            thread = threading.Thread(target=get_followings, args=(output_table, user_id, max_following))
            thread_list.append(thread)
            thread.start()
        for thread in thread_list:
            thread.join()
