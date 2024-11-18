#### Not tested in Hilton's AWS environment###
import os
import boto3
import json
import re
import numpy as np
import getpass
from typing import List
from .communication import pssh_execute

def get_internal_cluster_id() -> str:
    ''' Function to get cluster id from within the cluster (e.g. master node)
    
        Author(s): dliang1122@gmail.com
    '''
    try:
        # give notebook user permission to the emr status file
        current_user = getpass.getuser()
        os.system(f'sudo setfacl -R -m u:{current_user}:rx /emr/')

        # get cluster id info from emr job-flow.json
        with open('/emr/instance-controller/lib/info/job-flow.json','r') as f:
            cluster_id = json.load(f)['jobFlowId']

    except FileNotFoundError:
        cluster_id = ''

    return cluster_id


def get_node_ip(node_type: str = 'all') -> List[str]:
    ''' function to get node ips in an EMR cluster

        Author(s): dliang1122@gmail.com, kumarsajal49@gmail.com

        Args
        ------
        node_type (str): 'all' for all nodes; 'master' for master node only; 'core' for core nodes only

        Return
        -------
        list of node ips
    '''
    with open('/emr/instance-controller/lib/info/job-flow-state.txt') as f:
        state = f.read().strip()

    # get master node
    master_node_ip = re.search(".*masterHost: (.*)", state).group(1).strip('"')

    # get other nodes
    state = np.array(state.split("slaveRecords {"))
    state = np.delete(state, 0)
    core_node_ip = [re.search(".*privateIp: (.*)", items).group(1).strip('"') for items in state if 'RUNNING' in items]

    if node_type == 'all' or node_type == 'ALL':
        return [master_node_ip] + core_node_ip
    elif node_type == 'master' or node_type == 'MASTER':
        return master_node_ip
    elif node_type == 'core' or node_type == 'CORE':
        return core_node_ip
    else:
        raise ValueError('Invalid node_type, can only be MASTER(master), CORE(core) or ALL(all)')


def emr_change_cluster_size(target_instance_count: int, 
                            cluster_id: str = None, 
                            aws_region: str = 'us-east-1'):
    ''' function to change cluster size (number of nodes)

        Author(s): dliang1122@gmail.com

        Args
        ----------
        target_instance_count (int): target number of instances
        cluster_id(str): cluster id; if none, use id from the current cluster
        aws_region (string): aws region (e.g. us-east-1)

        Returns
        -------
        None
    '''
    # establish connection
    emr = boto3.client('emr', region_name = aws_region)

    if not cluster_id:
        # look up cluster id for the connected cluster if not provided
        cluster_id = get_internal_cluster_id()

    # look up core instance group id
    core_instance_group_id = [i['Id'] for i in emr.list_instance_groups(ClusterId = cluster_id)['InstanceGroups'] 
                              if i['InstanceGroupType'] == 'CORE'][0]

    # modify instance count
    emr.modify_instance_groups(ClusterId=cluster_id,
                               InstanceGroups = [{'InstanceGroupId': core_instance_group_id, 
                                                  'InstanceCount': target_instance_count}])


def terminate_cluster(cluster_id: str = None, 
                      aws_region: str = 'us-east-1'):
    ''' Function to terminate a cluster

        Author(s): dliang1122@gmail.com

        Args
        ----------
        cluster_id(str): cluster id; if none, terminate the current cluster
        aws_region (string): aws region (e.g. us-east-1)

        Returns
        -------
        None
    '''
    # establish connection
    emr = boto3.client('emr', region_name = aws_region)

    if not cluster_id:
        # look up cluster id for the connected cluster if not provided
        cluster_id = get_internal_cluster_id()

    emr.terminate_job_flows(JobFlowIds=[cluster_id])


def install_python_packages(package_or_ssh_git: str,
                            pkey: str,
                            nodes: List[str] = None,
                            venv_path: str = None,
                            git_branch = '',
                            user = 'hadoop',
                            verbose = True) -> None:
    ''' Function for install python packages to nodes in an EMR cluster
        
        Author(s): dliang1122@gmail.com

        Arg
        ------
        package_or_ssh_git (str): python package name or git path (ssh only; https not currently supported)
        pkey (str): location (can be an S3 path) of the EC2 key used for intra-cluster communication
        nodes (list): list of nodes to install package to
        venv_path (str): path of the virtual evironment to install package to
        git_branch (str): branch of git repo to install; only for installing git repo through ssh
        user (str): user account for the nodes
        verbose (bool): whether to print progress; will output stdout from all the nodes.
        
        
        Return
        -------
        None
    '''
    
    # process ssh git
    if 'ssh://git' in package_or_ssh_git:
        
        # if the string ends with '/', remove it
        if package_or_ssh_git[-1] == '/':
            package_or_ssh_git = package_or_ssh_git[0:-1]

        # extract repo name
        package_name = os.path.basename(package_or_ssh_git)

        # clone path for the git repo
        package_path = os.path.join('/tmp', package_name) + '/'

        # add branch argument to git clone if specified
        if git_branch:
            git_branch = f'-b {git_branch}'
        else:
            git_branch = ''

        clone_command = f'''git clone {git_branch} {package_or_ssh_git} {package_path};'''

        cleanup_command = f'''sudo rm -rf {package_path};'''

    elif 'https://git' in package_or_ssh_git:
        raise ValueError('https git not currently supported.')
    
    else:
        # not git repo
        package_path = package_or_ssh_git
        clone_command = ''
        cleanup_command = ''
        
    # handle virtual env
    if venv_path:
        install_command = f'''source {os.path.join(venv_path, 'bin/activate')}; pip3 install -U {package_path};'''
    else:
        install_command = f'''sudo pip3 install -U {package_path};'''
    
    # assemble commands
    command = clone_command + install_command + cleanup_command
    
    # if nodes arg is not set, get all nodes
    if not nodes:
        nodes = get_node_ip()
    
    # push command to all nodes
    pssh_execute(command = command, hosts = nodes, pkey = pkey, user = user, print_stdout = verbose, print_hosts = None)