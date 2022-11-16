from app.api.connect_password import password_details
from app.api.live_models import automate_cube_creation


def main():
    tenant = input("Enter the tenant name: ")
    case = input("Enter the use case: ")
    dev = input("Enter the environment (dev/ stage/ prod): ")
    model_name = input("(Note: this will be added at the last of the datamodel name) \nEnter the model name: ")
    print("Enter the table names using comma as seperator:")
    table_list = list(map(str, input().split(', ')))

    parameter = {
        "datamodel_name": f"{tenant}_{case}_{dev}_{model_name}",
        "dataset_name": f"{tenant}_{case}_{dev}_{model_name}_set",
        "provider": "RedShift",
        "table_list": table_list
    }

    password_detail = password_details(tenant)

    parameter.update(password_detail)
    # automate_cube_creation(parameter)
    print(parameter)

if __name__ == '__main__':
    main()