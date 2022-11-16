import requests
from app.setup_structlog import get_logger
from live_models import automate_cube_creation
from app import pid, api_key, password_url, port_num

_logger = get_logger(__name__)


def retrieve_values_from_password(password: dict):
    retrieved_password = {}
    server = ''
    for each_generic in password.get('GenericFieldInfo'):
        if each_generic['DisplayName'] == 'SERVER':
            server = each_generic['Value']

    retrieved_password['server'] = server.split('/')[0] + ':' + port_num
    retrieved_password['username'] = password.get('UserName')
    retrieved_password['password'] = password.get('Password')
    retrieved_password['defaultdatabase'] = retrieved_password['database'] = server.split('/')[1]
    retrieved_password['schema'] = (password.get('Description').split(':')[1])[:23]

    return retrieved_password


def get_password(datalake_user: str):
    _logger.info('Fetching Password from the password portal')

    if not pid or not api_key or not password_url:
        raise ValueError("Missing required parameters for get_password ..")

    headers = {
        "APIKey": api_key
    }
    try:
        _logger.info(f"Checking if password already exists: {datalake_user}")
        response = requests.get("{}/api/passwords/{}?QueryAll=true".format(password_url, pid), headers=headers).json()

        if len(response) > 0:
            passwords = [name for name in response if name.get("Title") == datalake_user]
            if passwords:
                _logger.info("Password already exists")
                _logger.info("Details for the table has been retrieved")
                return retrieve_values_from_password(passwords[0])
            return None

    except Exception as e:
        raise Exception(f"An error occurred while fetching the password - {format(str(e))}")
    else:
        return None


def password_details(tenant: str):
    datalake_user = f'{tenant}_price_datalake_dev_datalake_user'
    return get_password(datalake_user)


def start():
    tenant = input("Enter the tenant name: ")
    case = input("Enter the use case: ")
    dev = input("Enter the environment (dev/ test/ stage/ prod): ")
    model_name = input("(Note: this will be added at the last of the datamodel name) \nEnter the model name: ")
    region = input("Enter the region (US/ EU): ")
    num = int(input(
        "Choose the table number: \n1. sales \n2. npc \n3. phc\n"))

    table: dict = {
        "sales": ["v_fact_sales_bi"],
        "npc": ["v_npcomparison_migration_fact_bi"],
        "phc": ["v_phcomparison_fact_bi"]
    }

    parameter = {
        "datamodel_name": f"{tenant}_{case}_{dev}_{model_name}",
        "dataset_name": f"{tenant}_{case}_{dev}_{model_name}",
        "provider": "RedShift",
        "table_list": table[num]
    }

    parameter.update(password_details('agco'))

    automate_cube_creation(parameter)


start()
