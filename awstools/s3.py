import boto3
import os
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from urllib.parse import urlparse
from botocore.config import Config
from typing import List, Tuple, Generator
import json
import pickle
import random
import time
import re
from math import ceil, floor

import botocore
from botocore.exceptions import ClientError, NoCredentialsError
from aiohttp.client_exceptions import ServerTimeoutError
from fsspec.exceptions import FSTimeoutError
from botocore.parsers import ResponseParserError


#### function for dealing with s3 buckets
def parse_s3_path(s3_path: str) -> Tuple[str]:
    '''simple function for breaking an S3 URI into bucket and key

    Author(s): dliang1122@gmail.com

    Args
    ----------
    s3_path (str): full s3 path (e.g. s3://{Bucket}/{Prefix}). If not empty, use this instead of Bucket and Prefix

    Returns
    -------
    tuple of str: bucket name and key name
    '''
    # simple function for separating bucket from key or prefix for a s3 path
    parsed = urlparse(s3_path)

    bucket = parsed.netloc
    key = parsed.path
    if key:
        if key[0] == '/':
            key = key[1:]

    return bucket, key


def get_s3_client(aws_credential: dict = None, s3_retries: int = 20):
    '''A simple function for getting s3 client

        Author(s): dliang1122@gmail.com

        Args
        ----------
        aws_credential (dict): a dictionary containing aws credential key value pairs. E.g
                                {'aws_access_key_id': ACCESS_KEY,
                                'aws_secret_access_key': SECRET_KEY,
                                'aws_session_token': SESSION_TOKEN}
        s3_retries (int): number of retry for s3 operation

        Returns
        -------
        boto3 s3 client
    '''

    config = Config(
        retries={
            'max_attempts': s3_retries,
            'mode': 'standard'
        }
    )

    # establish connections
    if aws_credential:
        s3_client = boto3.client('s3',
                                 config=config,
                                 **aws_credential)
    else:
        s3_client = boto3.client('s3', config=config)

    return s3_client


def list_s3_buckets(aws_credential: dict = None):
    '''function for listing s3 buckets in the account'''
    s3 = get_s3_client(aws_credential = aws_credential)
    response = s3.list_buckets()

    return [i['Name'] for i in response['Buckets']]


def _s3_paginator(s3_path: str,
                  aws_credential: dict = None,
                  delimiter: str = '',
                  s3_retries: int = 20) -> botocore.paginate.PageIterator:
    '''get paginator for a prefix

    Author(s): dliang1122@gmail.com

    Args
    ----------
    s3_path (str): full s3 path (e.g. s3://{Bucket}/{Prefix}). If not empty, use this instead of Bucket and Prefix
    aws_credential (dict): a dictionary containing aws credential key value pairs. E.g
                            {'aws_access_key_id': ACCESS_KEY,
                             'aws_secret_access_key': SECRET_KEY,
                             'aws_session_token': SESSION_TOKEN}
    return_full_path (str): toggle for returning full s3 paths or just item names
    s3_retries (int): number of retry for s3 operation

    Returns
    -------
    list of str: list of s3 paths(keys)
    '''
    Bucket, Prefix = parse_s3_path(s3_path)

    s3 = get_s3_client(aws_credential = aws_credential,
                       s3_retries = s3_retries)

    paginator = s3.get_paginator('list_objects_v2')  # use paginator to loop through pages since boto3 will only show 1000 items max
    pag_iter = paginator.paginate(Bucket=Bucket,
                                  Prefix=Prefix,
                                  Delimiter = delimiter)
    
    return pag_iter


def list_s3_dir(s3_path: str,
                   aws_credential: dict = None,
                   return_full_path: bool = True,
                   s3_retries: int = 20) -> botocore.paginate.PageIterator:
    '''list s3 files and folders for the prefix specified; only return 1 level; does not return sub-folders and their files

    Author(s): dliang1122@gmail.com

    Args
    ----------
    s3_path (str): full s3 path (e.g. s3://{Bucket}/{Prefix}). If not empty, use this instead of Bucket and Prefix
    aws_credential (dict): a dictionary containing aws credential key value pairs. E.g
                            {'aws_access_key_id': ACCESS_KEY,
                             'aws_secret_access_key': SECRET_KEY,
                             'aws_session_token': SESSION_TOKEN}
    return_full_path (str): toggle for returning full s3 paths or just item names
    s3_retries (int): number of retry for s3 operation

    Returns
    -------
    list of str: list of s3 paths(keys)
    '''
    # make sure s3_path (assume directory) has the right format for a directory (end with /)

    s3_path = os.path.join(s3_path, '')
    
    Bucket, Prefix = parse_s3_path(s3_path)

    pag_iter = _s3_paginator(s3_path = s3_path, 
                             aws_credential = aws_credential, 
                             s3_retries = s3_retries, 
                             delimiter = '/')

    result = []

    for p in pag_iter:
        # get prefixes (directories)
        prefixes = p.get('CommonPrefixes')

        if prefixes:
            result.extend([d['Prefix'] for d in prefixes if d['Prefix']])

        # get keys (objects)
        keys = p.get('Contents')

        if keys:
            result.extend([d['Key'] for d in keys if d['Key'] != Prefix])
            
    if return_full_path:
        result = [os.path.join('s3://', Bucket, p) for p in result]
    else:
        result = [p.replace(Prefix, '') for p in result]

    return result


def list_s3_objects(s3_path: str = '',
                    aws_credential: dict = None,
                    ignore_dir: bool = True,
                    return_full_path: bool = True,
                    s3_retries: int = 20) -> List[str]:
    '''function for listing files in a prefix(folder); works for number of objects > 1000
       fast for small number of files but slow for deep multi-level structure

        Author(s): dliang1122@gmail.com

        Args
        ----------
        s3_path (str): full s3 path (e.g. s3://{Bucket}/{Prefix}). If not empty, use this instead of Bucket and Prefix
        aws_credential (dict): a dictionary containing aws credential key value pairs. E.g
                                {'aws_access_key_id': ACCESS_KEY,
                                 'aws_secret_access_key': SECRET_KEY,
                                 'aws_session_token': SESSION_TOKEN}
        ignore_dir (bool): whether to return directories or not
        return_full_path (bool): toggle for returning full s3 paths or just item prefixes
        s3_retries (int): number of retry for s3 operation

        Returns
        -------
        (list of str) list of object paths
    '''
    pag_iter = _s3_paginator(s3_path = s3_path, 
                             aws_credential = aws_credential, 
                             s3_retries = s3_retries)

    result = []
    for p in pag_iter:

        # get keys (objects)
        keys = p.get('Contents')

        if keys:
            if ignore_dir:
                object_list = [d['Key'] for d in keys if not d['Key'].endswith('/')]
            else:
                object_list = [d['Key'] for d in keys]

            result.extend(object_list)

    Bucket, Prefix = p['Name'], p['Prefix']

    if return_full_path:
        result = [os.path.join('s3://', Bucket, p) for p in result]
    else:
        result = [p.replace(Prefix, '') for p in result if p != Prefix]

    return result


def list_s3_objects_v2(s3_path: str = '',
                       aws_credential: dict = None,
                       ignore_dir: bool = True,
                       return_full_path: bool = True,
                       n_worker = 2 * (os.cpu_count() - 1),
                       s3_retries: int = 20) -> List[str]:
    '''Retrieve all files (paths) in a s3 prefix(folder) using recursion and multiprocessing; 
       fast for deep multi-level structure but slow for small number of files

        Author(s): dliang1122@gmail.com

        Known Issue: cause machines to crash when trying to list s3 prefixes with too many levels and objects (> 5 million);
                     likely cause: too many processes spawned. Use the much slower v0 version instead when dealing with too many objects and levels
        To Do: put a limit on number of processes to spawn.

        Args
        ----------
        s3_path (str): full s3 path (e.g. s3://{Bucket}/{Prefix}). If not empty, use this instead of Bucket and Prefix
        aws_credential (dict): a dictionary containing aws credential key value pairs. E.g
                                {'aws_access_key_id': ACCESS_KEY,
                                 'aws_secret_access_key': SECRET_KEY,
                                 'aws_session_token': SESSION_TOKEN}
        ignore_dir (bool): whether to return directories or not
        return_full_path (bool): toggle for returning full s3 paths or just item prefixes
        s3_retries (int): number of retry for s3 operation

        Returns
        -------
        (list of str) return the list of object file paths
    '''
    subfolders, files = [], []
    
    items = list_s3_dir(s3_path = s3_path, return_full_path = True, aws_credential = aws_credential)
    
    if not items:
        return list_s3_objects(s3_path = s3_path,
                                   aws_credential = aws_credential,
                                   ignore_dir = ignore_dir,
                                   return_full_path = return_full_path,
                                   s3_retries = s3_retries)
    
    for p in items:
        if p.endswith('/'):
            # sometimes the input directory will also be listed, exclude itself; otherwise, will run into infinite recursion issue.
            self_dir = s3_path if s3_path.endswith('/') else s3_path + '/'
            if p != self_dir:
                subfolders.append(p)
        else:
            files.append(p)

    if subfolders:
        #case1: subfolder > number of workers; assign 1 worker to each subfolder
        if len(subfolders) >= n_worker:
            futures = []
            with ProcessPoolExecutor(n_worker) as exe:
                for f in subfolders:
                    futures.append(exe.submit(list_s3_objects,
                                              s3_path = f,
                                              ignore_dir = ignore_dir,
                                              return_full_path = return_full_path,
                                              aws_credential = aws_credential,
                                              s3_retries = s3_retries)
                                  )

            files.extend([i for j in futures for i in j.result()])

        # case2: subfolder <= number of workers; To fully use the resources, we recursively distribute workers to sub-folders until all workers are assigned.
        elif len(subfolders) < n_worker:

            # assign number of workers to each sub-folder
            n_assigned = [floor(n_worker/len(subfolders))] * len(subfolders)
            for i in range(n_worker - sum(n_assigned)):
                n_assigned[i] += 1
            
            # assign workers to the subfolders in the next level recursively until we assign all the workers
            futures = []
            with ThreadPoolExecutor(len(subfolders)) as exe:
                for i in range(len(subfolders)):
                    futures.append(exe.submit(list_s3_objects_v2,
                                              s3_path = subfolders[i], 
                                              aws_credential = aws_credential, 
                                              ignore_dir = ignore_dir,
                                              return_full_path = return_full_path,
                                              n_worker = n_assigned[i],
                                              s3_retries = s3_retries)
                                  )

            files.extend([i for j in futures for i in j.result()])
            
    return sorted(set(files)) 


def _delete_single_batch(s3_client,
                         batch: List[str],
                         aws_credential: dict = None):
    '''helper file for deleting multiple objects; objects in the batch must be in the same bucket.

       Author(s): dliang1122@gmail.com
    '''
    Bucket = parse_s3_path(batch[0])[0]
    
    keys = [parse_s3_path(b)[1] for b in batch]
    
    max_retries = 10
    retry = 0
    while retry < max_retries:
        try:
            s3_client.delete_objects(Bucket = Bucket, 
                                     Delete={'Objects': [{'Key': b} for b in keys],
                                             'Quiet': True})
            return []
        except (ClientError, FileNotFoundError, OSError, NoCredentialsError, ServerTimeoutError, FSTimeoutError,
                ResponseParserError):
            time.sleep(random.randint(5, 20) / 10)
            retry += 1
    else:
        # time out
        return batch

        
def _delete_multiple_batches(s3_client,
                             batches: List[List[str]],
                             aws_credential: dict = None,
                             s3_retries = 20):
    '''helper file for deleting multiple batches of objects; objects in the each batch must be in the same bucket.

       Author(s): dliang1122@gmail.com
    '''

    for batch in batches:
        _delete_single_batch(s3_client = s3_client,
                             batch = batch,
                             aws_credential = aws_credential)

        
def _partition_list(lst, partition_size):
    if isinstance(partition_size, int):
        return [lst[k:(k + partition_size)] for k in range(0, len(lst), partition_size)]
    
    if isinstance(partition_size, (list, tuple)):
        for idx, n in enumerate(partition_size):
            if idx == 0:
                result = [lst[k:(k + n)] for k in range(0, len(lst), n)]
            else:
                result = [result[k:(k + n)] for k in range(0, len(result), n)]
    
        return result

    
def delete_s3_objects(s3_path: str,
                      max_retries: int = 10,
                      aws_credential: dict = None,
                      batch_size: int = 10000) -> None:

    # get items to copy
    paths = list_s3_objects_v2(s3_path = s3_path,
                               ignore_dir = False,
                               aws_credential = aws_credential,
                               return_full_path = True)

    random.shuffle(paths)

    n_objects = len(paths)
    print(f'Total Number of objects to delete: {n_objects}')
    
    s3_client = get_s3_client(aws_credential = aws_credential)
    
    not_deleted = paths
    
    retry = 0
    while not_deleted and retry < max_retries:
    
        worker_batches = _partition_list(not_deleted, partition_size = (1000, max(batch_size//1000, 1)))

        futures = []
        with ThreadPoolExecutor(len(worker_batches)) as exe:

            for batches in worker_batches:
                futures.append(exe.submit(_delete_multiple_batches, 
                                          s3_client,
                                          batches = batches,
                                          aws_credential = aws_credential,
                                          s3_retries = 20)
                              )
        
        not_deleted = list_s3_objects(s3_path = s3_path,
                                      ignore_dir = False,
                                      aws_credential = aws_credential,
                                      return_full_path = True)
        
        retry += 1
    
    if not_deleted:
        print(f'{n_objects - len(not_deleted)}/{n_objects} files deleted. Failed to delete the following objects.')
        print(not_deleted)
    else:
        print(f'{n_objects}/{n_objects} objects deleted.')


def _copy_single_item(s3_client, source, destination):
    '''function for copying single item from source s3 path to destination s3 path'''
    source_bucket, source_key = parse_s3_path(source)
    destination_bucket, destination_key = parse_s3_path(destination)
    
    s3_client.copy(CopySource={'Bucket': source_bucket, 'Key': source_key},
                   Bucket = destination_bucket,
                   Key = destination_key)


def _copy_multiple_items(source_list, 
                         destination_list, 
                         n_thread = 25):
    '''function for copying multiple items from source s3 paths to destination s3 paths using multi-threading'''
    # get s3 client
    s3_client = get_s3_client()
    
    futures = []

    # multithreaded copy
    with ThreadPoolExecutor(n_thread) as exe:
        for i in range(len(source_list)):
            futures.append(exe.submit(_copy_single_item,
                                      s3_client = s3_client,
                                      source = source_list[i],
                                      destination= destination_list[i])
                          )

    failed = []
    for idx, future in enumerate(futures):
        if future.exception():
            failed.append(source_list[idx])
    
    return failed


def copy_s3_items(source_s3_path: str,
                  destination_s3_path: str,
                  max_retries: int = 10,
                  aws_credential: dict = None,
                  n_workers: int = os.cpu_count()) -> None:
    '''function for copying s3 items using mutiple threads to speed up process

    Contact: dliang1122@gmail.com

    Args
    ----------
    source_s3_path (str): full s3 path (e.g. s3://{Bucket}/{Prefix or key}) for source folder/item
    destintation_s3_path (str): full s3 path (e.g. s3://{Bucket}/{Prefix or key}) for destination
    max_retries (int): number of retries
    aws_credential (dict): aws credentials
    n_worker (int): number of processes

    Returns
    -------
    None
    '''
    # get items to copy
    source_paths = list_s3_objects_v2(source_s3_path,
                                      ignore_dir = True,
                                      aws_credential = aws_credential,
                                      return_full_path = True)

    n_objects = len(source_paths)
    print(f'Total Number of files to copy: {n_objects}')
    
    not_copied = source_paths
    retry = 0
    
    while not_copied and retry < max_retries:

        # construct destination paths
        if len(not_copied) == 1:
            if destination_s3_path.endswith('/'): # if destination provided ends with a /, assume it's a directory 
                # destination path is destination_s3_path + file name
                destination_paths = [os.path.join(destination_s3_path, os.path.basename(not_copied[0]))]
            else: # otherwise, assume the destination_s3_path give is a file path
                destination_paths = [destination_s3_path]

        else:
            random.shuffle(not_copied)
            destination_paths = [''] * len(not_copied)

            for idx, source_path in enumerate(not_copied):
                # construct destination key by replacing the prefix
                if destination_s3_path.endswith('/'):
                    source_folder_name = os.path.dirname(source_s3_path.rstrip('/'))
                    destination_paths[idx] = source_path.replace(source_folder_name, destination_s3_path.rstrip('/'))
                else:
                    destination_paths[idx] = source_path.replace(source_s3_path.rstrip('/'), destination_s3_path.rstrip('/'))

        # create batches for parallel processing
        batch_size = len(not_copied) // n_workers

        source_batches = [not_copied[k:(k + batch_size)] for k in range(0, len(not_copied), batch_size)]
        destination_batches = [destination_paths[k:(k + batch_size)] for k in range(0, len(not_copied), batch_size)]

        futures = []
        # peform copy using multiprocessing
        with ProcessPoolExecutor(n_workers) as exe:
            for i in range(len(source_batches)):
                futures.append(exe.submit(_copy_multiple_items,
                                          source_list = source_batches[i], 
                                          destination_list = destination_batches[i])
                              )

        not_copied = []

        for future in futures:
            not_copied.extend(future.result())
        
        retry += 1

    if not_copied:
        print(f'{n_objects - len(not_copied)}/{n_objects} files copied. Failed to copy the following files.')
        print(not_copied)
    else:
        print(f'{n_objects}/{n_objects} files copied.')


def download_s3_files(s3_path: str,
                      destination: str,
                      max_retries = 10,
                      aws_credential: dict = None,
                      n_workers: int = 20) -> str:
    '''function to get file from s3 to local

    Contact: dliang1122@gmail.com

    Args
    ----------
    s3_path (str): location of the s3 objects; can be a folder or a file path
    destintation (str): local folder for storing the object

    Returns
    -------
    str: destination file path
    '''
    file_paths = list_s3_objects(s3_path = s3_path,
                                 ignore_dir = True,
                                 return_full_path = True,
                                 aws_credential = aws_credential)

    if not file_paths:
        raise FileNotFoundError('No files found in {s3_path}!')

    n_items = len(file_paths)
    print(f'Total Number of files to download: {n_items}')

    destination = os.path.expanduser(destination)

    if len(file_paths) == 1: # only 1 file
        filename = os.path.basename(file_paths[0]) # extract name of file
        if os.path.isdir(destination) or destination.endswith('/'):
            # set destination to destination/filename if destination is a folder
            destination_paths = [os.path.join(destination, filename)]
        else:
            # destination is a file path
            destination_paths = [destination]

    else: # s3_path given is only a prefix
        # get an example file path to extract the folder path since prefix might not be a complete folder path (ends with /)
        file_path_sample = file_paths[0]

        s3_path = s3_path.rstrip('/') # remove ending / to avoid string search complication
        folder_search = re.search(f'{s3_path}(.*?)(/)', file_path_sample) # search the string between s3_path and /

        if folder_search: # s3_path is a folder
            source_folder = folder_search.group(0) # get dir by obtaining the string between s3_path and / including s3_path
        else:
            source_folder = os.path.dirname(file_path_sample)

        if not destination.endswith('/'):
            destination = destination + '/'

        if not source_folder.endswith('/'):
            source_folder = source_folder + '/'

        # replace source folder with destination folder
        destination_paths = [p.replace(source_folder, destination) for p in file_paths]

    # download file
    s3 = get_s3_client(aws_credential = aws_credential,
                       s3_retries = 20)

    futures = [] # keep track of futures from multithreading process
    not_downloaded = file_paths
    retry = 0
    while not_downloaded and retry < max_retries: # keep retrying until all are downloaded or reaching max number of retries
        with ThreadPoolExecutor(max_workers = n_workers) as exe:
            for i in range(len(not_downloaded)):
                # get bucket and key from s3 path
                bucket, key = parse_s3_path(not_downloaded[i])
                destination_folder = os.path.dirname(destination_paths[i])
                if not os.path.exists(destination_folder):
                    os.makedirs(destination_folder)
                futures.append(exe.submit(s3.download_file, bucket, key, destination_paths[i]))

        # check if any failed
        temp = []
        for idx, future in enumerate(futures):
            if future.exception():
                temp.append(not_downloaded[idx])

        not_downloaded = temp
        retry += 1

    n_failed = len(not_downloaded)
    if n_failed > 0:
        print(f'{n_items - n_failed}/{n_items} files downloaded. Failed to download the following files.')
        print(not_downloaded)
    else:
        print(f'{n_items}/{n_items} files downloaded.')


def upload_files_to_s3(file_or_dir_paths,
                       s3_path: str,
                       include_hidden_files: bool = False,
                       aws_credential: dict = None,
                       n_workers:int = 20,
                       max_retries = 20) -> None:
    '''Save an object to s3

    Contact: dliang1122@gmail.com
    """
    '''

    file_paths = []

    if isinstance(file_or_dir_paths, str):
        file_or_dir_paths = [file_or_dir_paths]

    if isinstance(file_or_dir_paths, (list, tuple)):
        for path in file_or_dir_paths:
            if os.path.isdir(path):
                # if given directory path
                path = os.path.join(path, '') # add '/' to end of path if not specified
                paths = [os.path.join(dir_path, f) for dir_path, direc, files in os.walk(path) for f in files]

                if not include_hidden_files:
                    paths = [p for p in paths if not os.path.basename(p).startswith('.')]

                # generate destination s3 paths
                if s3_path.endswith('/'):
                    target_folder = os.path.join(s3_path, os.path.basename(path.rstrip('/')))
                    destination_paths = [p.replace(path.rstrip('/'), target_folder) for p in paths]
                else:
                    # generate destination s3 paths
                    destination_paths = [p.replace(path.rstrip('/'), s3_path) for p in paths]

                file_paths += list(zip(paths, destination_paths))
            elif os.path.isfile(path):
                # if given file path
                destination_path = os.path.join(s3_path, os.path.basename(path))

                file_paths.append((path, destination_path))
            else:
                raise FileNotFoundError(f"No such file or directory: '{path}'")
    else:
        raise Exception('Wrong format for "file_or_dir_paths"')

    n_items = len(file_paths)
    print(f'Total Number of files to upload: {n_items}')

    # save object to s3
    s3 = get_s3_client(aws_credential = aws_credential,
                       s3_retries = 20)

    futures = [] # keep track of futures from multithreading process
    failed = file_paths
    retry = 0
    while failed and retry < max_retries: # keep retrying until all are downloaded or reaching max number of retries
        with ThreadPoolExecutor(max_workers = n_workers) as exe:
            for source, destination in file_paths:
                destination_bucket, destination_key = parse_s3_path(destination)

                if destination_key.endswith('/'):
                    destination_key = os.path.join(destination_key, os.path.basename(source))

                futures.append(exe.submit(s3.upload_file, 
                                          Filename = source, 
                                          Bucket = destination_bucket, 
                                          Key = destination_key))

        # check if any failed
        temp = []
        for idx, future in enumerate(futures):
            if future.exception():
                temp.append(failed[idx])

        failed = temp
        retry += 1

    n_failed = len(failed)
    if n_failed > 0:
        print(f'{n_items - n_failed}/{n_items} files uploaded. Failed to upload the following files.')
        print(failed)
    else:
        print(f'{n_items}/{n_items} files uploaded.')


def get_s3_object(s3_path: str, aws_credential: dict = None, s3_retries = 20) -> object:
    '''get content of an s3 object

    Contact: dliang1122@gmail.com

    Args
    ----------
    s3_path (str): s3 path

    Returns
    -------
    botocore.response.StreamingBody: boto3 StreamingBody object
    """
    '''
    bucket, key = parse_s3_path(s3_path)

    s3 = get_s3_client(aws_credential = aws_credential,
                       s3_retries = s3_retries)
    obj = s3.get_object(Bucket=bucket, Key=key)

    return obj['Body']


def put_object_to_s3(obj, s3_path: str, aws_credential: dict = None, s3_retries = 20) -> None:
    '''Save an object to s3

    Contact: dliang1122@gmail.com
    """
    '''
    bucket, key = parse_s3_path(s3_path)

    # save object to s3
    s3 = get_s3_client(aws_credential = aws_credential,
                       s3_retries = s3_retries)
    s3.put_object(Bucket=bucket,
                  Key=key,
                  Body=obj)


def save_json_to_s3(content, s3_path: str, aws_credential: dict = None, s3_retries = 20) -> None:
    '''save an object in json to s3

    Contact: dliang1122@gmail.com
    '''

    # serialize content using json
    content_obj = json.dumps(content)
    put_object_to_s3(obj = content_obj, s3_path = s3_path, aws_credential = aws_credential, s3_retries = s3_retries)


def load_json_from_s3(s3_path: str, aws_credential: dict = None, s3_retries: int = 20) -> dict:
    '''load a json file from s3

    Contact: dliang1122@gmail.com
    '''
    obj = get_s3_object(s3_path = s3_path, aws_credential = aws_credential, s3_retries = s3_retries)

    return json.load(obj)


def save_pickle_to_s3(content, s3_path: str, aws_credential: dict = None, s3_retries: int = 20) -> None:
    '''save an object in pickel to s3

    Contact: dliang1122@gmail.com
    '''
    # pickle content
    content_obj = pickle.dumps(content)

    put_object_to_s3(content = content_obj, s3_path = s3_path, aws_credential = aws_credential, s3_retries = s3_retries)


def load_pickle_from_s3(s3_path: str, aws_credential: dict = None, s3_retries: int = 20) -> object:
    '''load a pickle file from s3

    Contact: dliang1122@gmail.com
    '''
    obj = get_s3_object(s3_path = s3_path, aws_credential = aws_credential, s3_retries = s3_retries)

    return pickle.loads(obj.read())


def s3_object_exists(s3_path: str, aws_credential: dict = None) -> bool:
    '''function for check whether an s3 object exists; works for s3 objects only (actual files), not for directory/prefix

    Contact: dliang1122@gmail.com

    Args
    ----------
    s3_path (str): full s3 path (e.g. s3://{Bucket}/{key}). If not empty, use this instead of Bucket and Prefix

    Returns
    -------
    bool: True or False
    '''
    # extract bucket and key from s3_path
    Bucket, Key = parse_s3_path(s3_path)

    s3 = get_s3_client(aws_credential = aws_credential)

    try:
        s3.head_object(Bucket = Bucket, Key = Key)
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False

    return True
