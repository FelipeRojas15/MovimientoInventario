import os
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# config.py

KAFKA_BOOTSTRAP_SERVERS = [os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')]
KAFKA_GROUP_ID = os.environ.get('KAFKA_GROUP_ID', 'grupo-2')

AWS_CONFIG = {
    'region_name': os.environ.get('AWS_REGION'),
    'aws_access_key_id': os.environ.get('AWS_ACCESS_KEY_ID'),
    'aws_secret_access_key': os.environ.get('AWS_SECRET_ACCESS_KEY')
}

DYNAMODB_TABLES = {
    'PRODUCTO': 'Producto',
    'MOVIMIENTO_INVENTARIO': 'MovimientoInventario'
}

SERVER_CONFIG = {
    'HOST': os.environ.get('SERVER_HOST', '0.0.0.0'),
    'PORT': int(os.environ.get('SERVER_PORT', 8080))
}