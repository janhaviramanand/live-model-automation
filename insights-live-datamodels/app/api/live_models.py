import requests
import app.api.table_handle as table_handle

from app.api.sisense_endpoints import get_endpoint
from app.setup_structlog import get_logger

_logger = get_logger(__name__)


def create_live_cube(parameter: dict):
    _logger.info('Creating live cube on LocalHost', Live_cube=parameter['datamodel_name'])
    endpoint, headers = get_endpoint('create_live_cube')
    data = {
        "title": parameter['datamodel_name'],
        "type": "live"
    }
    response = requests.post(url=endpoint, json=data, headers=headers)
    response.raise_for_status()
    _logger.info('Successfully created live cube on LocalHost')
    return response.json()["oid"]


def create_live_dataset(parameter: dict):
    _logger.info('Creating datasets for Live cube', Live_oid=parameter['datamodel_oid'])
    endpoint, headers = get_endpoint('create_live_dataset', parameter['datamodel_oid'])
    data = {
        "name": parameter["dataset_name"],
        "type": "live",
        "connection": {
            "provider": parameter["provider"],
            "parameters": {
                "ApiVersion": "2",
                "Server": parameter["server"],
                "UserName": parameter["username"],
                "Password": parameter["password"],
                "DefaultDatabase": parameter["defaultdatabase"],
                "EncryptConnection": "true",
                "TrustServerCertificate": "true",
                "AdditionalParameters": "",
                "Database": parameter["database"]
            },
            "schema": parameter["schema"],
            "timeout": 60000,
            "refreshRate": 30000,
            "resultLimit": 5000,
            "uiParams": {},
            "globalTableConfigOptions": None
        }
    }
    response = requests.post(url=endpoint, json=data, headers=headers)
    response.raise_for_status()
    _logger.info(f"Successfully added dataset to the datamodel {parameter['datamodel_name']}")
    return response.json()['oid']


def add_table(parameter):
    _logger.info(f'Adding tables {parameter["table_list"]}', datamodel_id=parameter['datamodel_oid'],
                 dataset_id=parameter['dataset_oid'])
    endpoint, headers = get_endpoint('create_live_table', parameter['datamodel_oid'], parameter['dataset_oid'])
    for table_name in parameter['table_list']:
        data = {
            "id": table_name,
            "name": table_name,
            "columns": [{
                "id": "c1",
                "name": "c1",
                "type": 8,
                "size": 10,
                "precision": 10,
                "scale": 0,
                "hidden": False,
                "indexed": True,
                "isUpsertBy": False
            }],
            "buildBehavior": {
                "type": "sync",
                "accumulativeConfig": None
            },
            "hidden": False,
            "description": None
        }
        response = requests.post(url=endpoint, json=data, headers=headers)
        response.raise_for_status()
        _logger.info(f"Dummy table added with name {table_name}")
        parameter[table_name] = response.json()['oid']

        table_handle.refresh_schema(parameter, table_name)


def automate_cube_creation(parameter: dict):
    _logger.info("Automated Creation based on the parameters")
    parameter['datamodel_oid'] = create_live_cube(parameter)
    parameter['dataset_oid'] = create_live_dataset(parameter)
    add_table(parameter)

