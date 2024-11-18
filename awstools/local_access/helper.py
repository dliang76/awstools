import sys
import boto3
import os
import requests
from configparser import RawConfigParser
import base64
import xml.etree.ElementTree as ET
import re
from bs4 import BeautifulSoup
from urllib import parse, request
import json
import webbrowser
from getpass import getpass
from securid.sdtid import SdtidFile
from typing import Optional, Dict, List, Union
from getpass import getpass


def get_rsa_passcode(sdtid_path: str,
                     password: Optional[str] = None,
                     pin: Optional[int] = None) -> int:
    '''function to get rsa passcode'''
    if not password:
        password = getpass('sdtid file password [press enter if none]: ')
        if not password:
            password = None

    if not pin:
        pin = getpass('RSA pin [press enter if none]: ')
        if not pin:
            pin = None
        else:
            pin = int(pin)

    sdtid = SdtidFile(sdtid_path)
    token = sdtid.get_token(password = password)
    return token.now(pin)


def get_SAML_response(username: str = None,
                      passcode: int = None,
                      ssl_verification: bool = True) -> str:
    '''function to get SAML response/token
       Borrowed code from https://jira.hilton.com/stash/scm/sea/ansible-aws-security.git
    '''

    if not username:
        username = input('User ID: ')

    if not passcode:
        passcode = getpass('RSA Passcode: ')


    # enter saml entry url
    idp_entry_url = 'https://fd.hilton.com/idp/startSSO.ping?PartnerSpId=urn:amazon:webservices&awsflag=console'
    requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS += ':RC4-SHA:!DH'
    session = requests.Session()
    response = session.get(idp_entry_url, verify = ssl_verification)

    # enter user name and passcode in the authorization form
    idp_auth_form_submit_url = response.url
    formsoup = BeautifulSoup(response.text, "html.parser") # get form
    payload = {}
    payload["pf.ok"]='  OK  '
    payload["pf.username"] = username
    payload["pf.pass"] = passcode

    # go through the form and locate user name and passcode fields
    for inputtag in formsoup.find_all(re.compile('(INPUT|input)')):
        name = inputtag.get('name','')
        value = inputtag.get('value','')
        if "user" in name.lower():
            #Make an educated guess that this is the right field for the username
            payload[name] = username
        elif "email" in name.lower():
            #Some IdPs also label the username field as 'email'
            payload[name] = username
        elif "pass" in name.lower():
            #Make an educated guess that this is the right field for the passcode
            payload[name] = passcode
        else:
            #Simply populate the parameter with the existing value (picks up hidden fields in the login form)
            payload[name] = value

    # submit entered user name and passcode
    for inputtag in formsoup.find_all(re.compile('(FORM|form)')):
        action = inputtag.get('action')
        if action:
            parsedurl = parse.urlparse(idp_entry_url)
            idp_auth_form_submit_url = parsedurl.scheme + "://" + parsedurl.netloc + action

    # get response from submitted credentials
    response = session.post(url = idp_auth_form_submit_url, data = payload, verify = ssl_verification)

    # Decode the response and extract the SAML assertion
    soup = BeautifulSoup(response.text, "html.parser" )
    assertion = ''

    # Look for the SAMLResponse attribute of the input tag (determined by
    # analyzing the debug print lines above)
    for inputtag in soup.find_all('input'):
        if(inputtag.get('name') == 'SAMLResponse'):
            #print(inputtag.get('value'))
            assertion = inputtag.get('value')

    # Better error handling is required for production use.
    if (assertion == ''):
        #TODO: Insert valid error checking/handling
        print ('Response did not contain a valid SAML assertion')
        sys.exit(0)

    return assertion


def get_authorized_roles(saml_response: str) -> List[Dict[str, str]]:
    '''function to get authorized IAM role for the login
       Borrowed code from https://jira.hilton.com/stash/scm/sea/ansible-aws-security.git
    '''
    # Parse the returned saml_response and extract the authorized roles
    awsroles = []
    root = ET.fromstring(base64.b64decode(saml_response)) # decode

    for saml2attribute in root.iter('{urn:oasis:names:tc:SAML:2.0:assertion}Attribute'):
        if (saml2attribute.get('Name') == 'https://aws.amazon.com/SAML/Attributes/Role'):
            for saml2attributevalue in saml2attribute.iter('{urn:oasis:names:tc:SAML:2.0:assertion}AttributeValue'):
                awsroles.append(saml2attributevalue.text)

    # parse out role_arn and principal_arn
    result = []
    for awsrole in awsroles:
        awsrole_dict = dict()
        for r in awsrole.split(','):
            if 'saml-provider' in r:
                awsrole_dict['principal_arn'] = r
            else:
                awsrole_dict['role_arn'] = r

        result.append(awsrole_dict)

    return result


def get_role_credentials(role_arn: str,
                         principal_arn: str,
                         saml_response: str,
                         duration_h: Union[int, float] = 8,
                         aws_region: int = 'us-east-1') -> Dict[str, str]:
    '''function to get temp aws credentials
    '''
    conn = boto3.client('sts', region_name = aws_region)
    credentials = conn.assume_role_with_saml(RoleArn = role_arn,
                                             PrincipalArn = principal_arn,
                                             SAMLAssertion = saml_response,
                                             DurationSeconds= int(3600 * duration_h))

    return credentials.get('Credentials')


def create_file(file_path: str) -> str:
    '''function to create an empty file given a file path'''
    file_path = os.path.expanduser(file_path)
    dir_path = os.path.dirname(file_path)

    if not os.path.exists(file_path):
        os.makedirs(dir_path, exist_ok = True)
        with open(file_path, 'a'):
            pass

    return file_path


def write_config(file_path, profile, **kwargs):

    # create config file if not exist
    file_path = create_file(file_path = file_path)

    # Read in the existing credentials file
    config = RawConfigParser()
    config.read(file_path)

    # create profile if not found
    if not config.has_section(profile):
        config.add_section(profile)

    for k,v in kwargs.items():
        config.set(profile, k, v)

    # Write to file
    with open(file_path, 'w+') as f:
        config.write(f)


def write_aws_config(profile: str,
                     iam_role: str,
                     aws_region = 'us-east-1',
                     **kwargs) -> None:
    '''function to write aws configuration to aws config file'''
    # create config file if not exist
    file_path = '~/.aws/config'

    if profile != 'default':
        profile = f'profile {profile}'

    write_config(file_path = file_path,
                 profile = profile,
                 output = 'json',
                 region = aws_region,
                 role = iam_role,
                 **kwargs)


def write_temp_credentials(profile: str,
                           aws_access_key_id: str,
                           aws_secret_access_key: str,
                           aws_session_token: str) -> None:
    '''function to write temp aws credentials aws credentials file'''
    # create credentials file if not exist
    file_path = '~/.aws/credentials'

    write_config(file_path = file_path,
                 profile = profile,
                 aws_access_key_id = aws_access_key_id,
                 aws_secret_access_key = aws_secret_access_key,
                 aws_session_token = aws_session_token)


def get_profile_config(profile: str) -> Dict[str, str]:
    '''function to get aws profile configuration'''
    file_path = os.path.expanduser('~/.aws/config')

    config = RawConfigParser()
    config.read(file_path)

    if profile != 'default':
        profile = f'profile {profile}'

    if not config.has_section(profile):
        return {}

    return dict(config[profile])


def set_aws_profile(profile: str) -> None:
    '''function to set aws profile'''

    file_path = os.path.expanduser('~/.aws/credentials')

    config = RawConfigParser()
    config.read(file_path)

    if not config.has_section(profile):
        raise ValueError('No such profile found! Please enter a different profile or create a new one through saml_login().')

    # set env variable (needed for pacakge like pandas)
    os.environ["AWS_PROFILE"] = profile

    # set boto3 profile so we don't have to include credentials every time we use boto3
    boto3.setup_default_session(profile_name = profile)


def open_aws_console(aws_access_key_id: str,
                     aws_secret_access_key: str,
                     aws_session_token: str,
                     browser: str = None) -> None:

    ''' Function to open aws web console using AWS credentials.
        Borrowed some code from https://gist.github.com/ottokruse/1c0f79d51cdaf82a3885f9b532df1ce5
    '''

    url_credentials = dict(sessionId = aws_access_key_id,
                           sessionKey = aws_secret_access_key,
                           sessionToken = aws_session_token)

    # get sign-in token
    request_url = "https://signin.aws.amazon.com/federation"
    request_url += "?Action=getSigninToken"
    # request_url += "&DurationSeconds=7200" # not actually needed; duration is determined during aws credential generation
    request_url += "&Session=" + parse.quote_plus(json.dumps(url_credentials))

    with request.urlopen(request_url) as response:
        if not response.status == 200:
            raise Exception("Failed to get federation token")
        signin_token = json.loads(response.read())

    # login
    request_url = "https://signin.aws.amazon.com/federation"
    request_url += "?Action=login"
    request_url += "&Destination=" + parse.quote_plus("https://console.aws.amazon.com/")
    request_url += "&SigninToken=" + signin_token["SigninToken"]

    if browser:
        webbrowser.get(browser).open(request_url, new = 1)
    else:
        webbrowser.open(request_url, new = 1)
