#### the glue module only works in an AWS Glue environment; not tested in Hilton's AWS enviornment
import boto3
import sys
from awsglue.utils import getResolvedOptions, GlueArgumentError

def get_workflow_params(aws_region: str = 'us-east-1') -> dict:
    ''' simple function for getting workflow parameters within a glue job

        Author(s): dliang1122@gmail.com

        Args
        ----------
        aws_region (str): aws region

        Returns
        -------
        dict: workflow parameters; returns an empty dictionary if the glue job is not part of a workflow
    '''
    # check if part of a workflow; if yes, retrieve parameters from workflow; otherwise, return a empty dictionary
    try:
        workflow_args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])
        glue_client = boto3.client("glue", region_name = aws_region)
        workflow_params = glue_client.get_workflow_run_properties(Name = workflow_args['WORKFLOW_NAME'],
                                                                  RunId = workflow_args['WORKFLOW_RUN_ID'])["RunProperties"]
    except GlueArgumentError:
        workflow_params = dict()

    return workflow_params

def update_workflow_runtime_params(param_dict: dict,aws_region: str = 'us-east-1') -> None:
    ''' simple function for updating workflow runtime parameters from a glue job

        Author(s): dliang1122@gmail.com

        Args
        ----------
        param_dict (dict): keys = parameter names, values = update values
        aws_region (str): aws region

        Returns
        -------
        None
    '''
    try:
        workflow_args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])
        glue_client = boto3.client("glue", region_name = aws_region)
        workflow_params = glue_client.get_workflow_run_properties(Name = workflow_args['WORKFLOW_NAME'],
                                                                RunId = workflow_args['WORKFLOW_RUN_ID'])["RunProperties"]

        # update parameter value
        workflow_params.update(param_dict)
        glue_client.put_workflow_run_properties(Name = workflow_args['WORKFLOW_NAME'],
                                                RunId = workflow_args['WORKFLOW_RUN_ID'],
                                                RunProperties = workflow_params)
    except:
        raise RuntimeError('Not part of a workflow!')


def update_workflow_default_params(param_dict: dict, aws_region: str = 'us-east-1') -> None:
    ''' simple function for updating workflow default parameters from a glue job

        Author(s): dliang1122@gmail.com

        Args
        ----------
        param_dict (dict): keys = parameter names, values = update values
        aws_region (str): aws region

        Returns
        -------
        None
    '''
    try:
        # retrieve workflow name
        workflow_name = getResolvedOptions(sys.argv, ['WORKFLOW_NAME'])['WORKFLOW_NAME']

        glue_client = boto3.client('glue', region_name = aws_region)

        # get original default run properties
        default_run_properties = glue_client.get_workflow( Name = workflow_name, IncludeGraph = False)['Workflow']['DefaultRunProperties']

        # overwrite using updated values
        default_run_properties.update(param_dict)

        # update workflow
        glue_client.update_workflow(Name= workflow_name, DefaultRunProperties = default_run_properties)

    except:
        raise RuntimeError('Not part of a workflow!')