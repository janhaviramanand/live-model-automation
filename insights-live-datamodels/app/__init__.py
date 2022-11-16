import os

hostname = os.getenv('SISENSE_URL', "https://insights.us.anls.syncroncloud.team")
aws_region = os.getenv('AWS_REGION', 'us-east-1')
prefix = os.getenv('PREFIX', 'insights')
token_expiry = os.getenv('TOKEN_EXPIRY', 15)
pid = os.getenv('PID', 1928)
api_key = os.getenv('API_KEY', 'd5e6b79384f3ca278eb2cd34a596f352')
password_url = os.getenv('PASSWORD_URL', "https://passwords.syncron.team")
port_num = os.getenv('PORT_NUMBER', '5439')