import argparse
from awstools.local_access import saml_login, aws_console_login
import os

def aws_auth():
    # parse cli arguments
    parser=argparse.ArgumentParser()
    parser.add_argument('-u','--username')
    parser.add_argument('-p','--passcode')
    parser.add_argument('-P', '--profile', default = 'default')
    parser.add_argument('-d', '--duration_h', default = 8)
    parser.add_argument('-R', '--force_renew', default = False)
    parser.add_argument('-r', '--aws_region', default = 'us-east-1')
    args=parser.parse_args()

    # saml login
    saml_login(username = args.username,
                passcode = args.passcode,
                profile = args.profile,
                duration_h = float(args.duration_h),
                force_renew = args.force_renew,
                aws_region = args.aws_region)
    
    # TO DO: Figure out how to let the parent shell know which aws profile to use; 
    # setting environment variable AWS_PROFILE in python script does not set the environment variable in the parent shell.
    # Need to think of something else.


def aws_console_auth():
    parser=argparse.ArgumentParser()
    parser.add_argument('-u','--username', default = None)
    parser.add_argument('-p','--passcode', default = None)
    parser.add_argument('-d', '--duration_h', default = 8)
    parser.add_argument('-r', '--aws_region', default = 'us-east-1')
    parser.add_argument('-b', '--browser', default = None)
    args=parser.parse_args()

    aws_console_login(username = args.username,
                      passcode = args.passcode,
                      duration_h = float(args.duration_h),
                      aws_region = args.aws_region,
                      browser = args.browser)




