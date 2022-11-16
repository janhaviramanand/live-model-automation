import boto3

from app import aws_region
from app.setup_structlog import get_logger

_logger = get_logger(__name__)


def get_ssm_client():
    return boto3.client('ssm', region_name=aws_region)


class ParameterNotFoundException(Exception):
    def __init__(self, name):
        self.name = name


def get_ssm_parameter(parameter_name):
    try:
        ssm_param = get_ssm_client().get_parameter(Name=parameter_name, WithDecryption=True)
        return ssm_param['Parameter']['Value']
    except Exception:
        _logger.critical('SSM parameter not found', parameter_name=parameter_name)
        raise ParameterNotFoundException(parameter_name)
