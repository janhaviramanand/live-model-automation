import requests

from app.api.sisense_endpoints import get_endpoint
from app.setup_structlog import get_logger

_logger = get_logger(__name__)


def get_recent_connections():
    _logger.info("Fetching the recent connection")
    endpoint, headers = get_endpoint('connections')
    response = requests.get(url=endpoint, headers=headers)
    return response.json()[-1]


def table_schema_data(parameter: dict, table_name: str):
    conn_oid = get_recent_connections().get("oid")

    _logger.info('Fetching details of the table based on connection', connection_id=conn_oid)
    endpoint, headers = get_endpoint('table_schema_details', conn_oid)
    data = {
        "provider": parameter['provider'],
        "connectionData": {
            "connection": {
                "ApiVersion": "2",
                "Server": parameter['server'],
                "UserName": parameter['username'],
                "DefaultDatabase": parameter['defaultdatabase'],
                "EncryptConnection": False,
                "AdditionalParameters": "",
                "Database": parameter['database']
            },
            "provider": parameter['provider'],
            "schema": parameter['schema'],
            "table": table_name
        }
    }

    response = requests.post(url=endpoint, json=data, headers=headers)
    #response.raise_for_status()
    return response.json()


def orient_columns(column_list: list):
    new_column_list = []
    for column in column_list:
        ex = {
            "id": column.get("columnName"),
            "name": column['columnName'],
            "type": column['dbType'],
            "size": column['size'],
            "precision": column['precision'],
            "scale": column['scale'],
            "hidden": False
        }
        new_column_list.append(ex)
    return new_column_list


def update_base_table(parameter: dict, table_name: str, column_list: list):
    _logger.info("Updating the dummy table to the base table")
    endpoint, headers = get_endpoint('ecm')
    data = {
        "operationName": "updateBaseTableSchema",
        "query": "mutation updateBaseTableSchema($elasticubeOid: UUID!, $table: TableInput!) {\n  table: updateBaseTableSchema(elasticubeOid: $elasticubeOid, table: $table) {\n    buildBehavior {\n      type\n      accumulativeConfig {\n        type\n        column\n        lastDays\n        keepOnlyDays\n        __typename\n      }\n      __typename\n    }\n    columns {\n      oid\n      id\n      name\n      type\n      size\n      precision\n      scale\n      hidden\n      indexed\n      isUpsertBy\n      __typename\n    }\n    tupleTransformations {\n      type\n      arguments\n      __typename\n    }\n    __typename\n  }\n}\n",
        "variables": {
            "elasticubeOid": parameter['datamodel_oid'],
            "table": {
                "id": table_name,
                "name": table_name,
                "schemaName": parameter['schema'],
                "hidden": False,
                "columns": column_list,
                "oid": parameter[table_name],
            }
        },
    }

    response = requests.post(url=endpoint, json=data, headers=headers)
    response.raise_for_status()
    _logger.info(f"Table {table_name} is refreshed", table_oid=parameter[table_name])
    return response.status_code


def refresh_schema(parameter: dict, table_name: str):
    meta_data = table_schema_data(parameter, table_name)
    columns_list = orient_columns(meta_data['columns'])
    print(update_base_table(parameter, table_name, columns_list))

# params = {
#     "datamodel_name": "agco_pm_dev_testmodel",
#     "dataset_name": "agco_pm_dev_testset",
#     "server": "syncron-dw-dev.cjeppnlvxazb.us-east-1.redshift.amazonaws.com:5439",
#     "username": "agco_price_datalake_dev_datalake_user",
#     "password": "kGi57&n:4EjGBGpr",
#     "defaultdatabase": "agco",
#     "database": "agco",
#     "provider": "RedShift",
#     "schema": "agco_price_datalake_dev",
#     "table_list": ["v_fact_sales_bi, v_phcomparison_fact_bi"] #["v_fact_sales, v_phcomparison_fact"]
# }
# refresh_schema(params, "v_fact_sales_bi")
