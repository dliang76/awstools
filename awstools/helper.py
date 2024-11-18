import boto3
import random
import time

def get_aws_credentials(max_retries: int = 20):
    '''method for retrieving AWS credentials'''
    
    retry = 0
    while retry < max_retries:
        try:
            # get aws credentials
            session = boto3.Session()
            credentials = session.get_credentials()
            aws_credentials = {'aws_access_key_id': credentials.access_key,
                               'aws_secret_access_key': credentials.secret_key,
                               'aws_session_token':credentials.token}
            break
        except Exception as e:
            print(e)
            retry += 1
            time.sleep(random.randint(5, 20) / 10)
    else:
        raise Exception(f'Could not load AWS credentials! Max number of retries reached ({max_retries}).')
    return aws_credentials