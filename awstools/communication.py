#### Not tested in Hilton's AWS environment###
from pssh.clients import ParallelSSHClient
from gevent import joinall
from typing import Union, List
import paramiko
import os
from .s3 import get_file_from_s3

def get_pkey(pkey_path: str,  destination_dir:str = '/tmp/ec2-keys/') -> str:
    ''' Function to get ssh private key from s3; typically used with EMR

        Author(s): dliang1122@gmail.com

        Args
        ----------
        pkey_path (str): ssh private key location; can be an s3 location
        destination_dir (str): destination directory if downloading pkey from s3

        Returns
        -------
        str: pkey values

        To Do:
            Automatically find the ssh key in S3 if pkey_path is not specified.
            Need to resolve permission issue first
            Need to access /home/hadoop/.ssh/authorized_keys, which cannot be read from the notebook
            without changing file permission; but, changing permission for the public key file messed up
            the system (can't login using pkey anymore)
    '''

    # download pkey to local if given s3 path
    if 's3://' in pkey_path:
        dest_dir_exists = os.path.isdir(destination_dir)
        pkey = get_file_from_s3(s3_loc = pkey_path, destination = destination_dir)

        if not dest_dir_exists:
            # all user can use the folder; avoid permission issue when switching between pyspark and python env
            os.chmod(destination_dir, 0o777)

        os.chmod(pkey, 0o400)
    else:
        pkey = pkey_path

    # if no private key found, raise FileNotFoundError
    if not os.path.isfile(pkey):
        raise FileNotFoundError('Private key file not found')

    return pkey


def create_pssh_session(hosts: Union[List[str], str], pkey: str, user:str = 'hadoop'):
    ''' Wrapper for setting up parallel ssh (pssh) connection with a private key

        Author(s): dliang1122@gmail.com


        Args
        ----------
        hosts (list): list of host ips
        pkey (str): location of ssh private key; can be an S3 path
        user (str): user name for ssh login; should be 'hadoop'

        Returns
        -------
        pssh client session
    '''
    if type(hosts) == str:
        hosts = [hosts]

    # download pkey to local if given s3 path
    pkey = get_pkey(pkey_path = pkey)

    return ParallelSSHClient(hosts = hosts, user = user, pkey = pkey)


def get_pssh_output(output, hosts = None):
    '''print out pssh shell output
        Author(s): dliang1122@gmail.com

    '''
    if type(hosts) == str:
        hosts = [hosts]

    for host_output in output:
        # If hosts arg is not set, print all outputs.
        # If hosts arg is provided, pritn only outputs for the specified hosts
        if not hosts or host_output.host in hosts:
            print(f'{host_output.host}: Exit code({host_output.exit_code})')
            for line in host_output.stdout:
                print(line)
            # print error if any
            for err in host_output.stderr:
                print(err)


def pssh_execute(command: str,
                 hosts: List[str],
                 pkey: str,
                 user: str = 'hadoop',
                 print_stdout: bool = True,
                 print_hosts: List[str] = None):
    '''wrapper function for sending commands to multiple hosts using pssh

        Author(s): dliang1122@gmail.com

        Args
        ----------
        command (str): shell command to run
        hosts (list): list of host ips
        pkey (str): location of ssh private key; can be an S3 path
        user (str): user name for ssh login; should be 'hadoop'
        print_stdout (boolean): whether to display outputs
        print_hosts (list): list of host ips for which we want to print outputs

        Returns
        -------
        None
    '''
    try:
        # get pkey
        pkey_loc = get_pkey(pkey_path = pkey)

        session = create_pssh_session(hosts = hosts, pkey = pkey_loc, user = user)

        result = session.run_command(command)

        if print_stdout:
            get_pssh_output(result, hosts = print_hosts)
    finally:
        if 's3://' in pkey:
            os.system(f'rm -f {pkey_loc}')


def create_ssh_session(host: str, pkey: str, user: str = 'hadoop'):
    '''function to create a ssh session using paramiko pacakge. 
       Only ssh implementation found to work with distributed script launcher

        Author(s): dliang1122@gmail.com

        Args
        ----------
        host (str): host ip
        pkey (str): location of ssh private key; can be an S3 path
        user (str): user name for ssh login; should be 'hadoop'

        Returns
        -------
        ssh client session
    '''
    # download pkey to local if given s3 path
    pkey = get_pkey(pkey_path = pkey)

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname = host, username = user, key_filename = pkey)

    return client


def ssh_execute(command: str,
                host: str, 
                pkey: str,
                user: str = 'hadoop', 
                print_stdout: bool = True,
                stdout_log: str = None,
                err_collect = []):
    '''wrapper function for sending command to host through ssh using paramiko pacakge

        Author(s): dliang1122@gmail.com, kumarsajal49@gmail.com

    Args
    ----------
    command (str): shell command to run
    host (str): host ip
    pkey (str): location of ssh private key; can be an S3 path
    user (str): user name for ssh login; should be 'hadoop'
    print_stdout (boolean): whether to display outputs
    stdout_log (str): log file pth; if specified, it records stdout

    Returns
    -------
    None
    '''
    try:
        # download pkey to local if given s3 path
        pkey_loc = get_pkey(pkey_path = pkey)

        # establish ssh session
        ssh_session = create_ssh_session(host = host, pkey = pkey_loc, user = user)

        # record output if specified
        if stdout_log:
            # clear file content
            open(stdout_log,"w").close()
            f = open(stdout_log,"a")


        stdin, stdout, stderr = ssh_session.exec_command(command)

        for line in stdout:
            if print_stdout:
                print(line)
            if stdout_log:
                f.write(line)
        # print error if any
        for err in stderr:
            err_collect.append(err)

    except Exception as e:
        print(e)

    finally:
        # clean up
        ssh_session.close()

        if 's3://' in pkey:
            os.system(f'rm -f {pkey_loc}')

        if stdout_log:
            f.close()
        

    return stdout.channel.recv_exit_status()


def sftp(local_file: str, 
         destination_file: str, 
         target_hosts: List[str], 
         pkey: str, 
         user: str = 'hadoop', 
         recurse: bool = False) -> None:
    '''function for transferring file using sftp implementation in pssh pacakge; support both both single or multiple target hosts
    
        Author(s): dliang1122@gmail.com

        Args
        ----------
        local_file (str): local file path
        destination_file (str): destination file path
        target_hosts (list of str): list of target ips
        pkey (str): pkey file path
        user (str): user name for ssh login; should be 'hadoop'
        recurse (boolean): required for copying folders

        Returns
        -------
        None
    '''

    # establish ssh session
    if type(target_hosts) == str:
        target_hosts = [target_hosts]

    try:
        # download pkey to local if given s3 path
        pkey_loc = get_pkey(pkey_path = pkey)

        session = create_pssh_session(hosts = target_hosts, user = user, pkey = pkey_loc)

        cmds = session.copy_file(local_file, destination_file, recurse = recurse)
        joinall(cmds, raise_error=True)

    except Exception as e:
        print(e)
    finally:
        if 's3://' in pkey:
            os.system(f'rm -f {pkey_loc}')

