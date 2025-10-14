from kafka import KafkaConsumer
from kafka import KafkaProducer
import json
import boto3
import uuid
from datetime import datetime
from decimal import Decimal
import config

# Inicializa DynamoDB
dynamodb = boto3.resource(
    'dynamodb',
    **config.AWS_CONFIG
)


def safe_json_deserializer(v):
    try:
        return json.loads(v.decode('utf-8'))
    except Exception as e:
        print(f"⚠️ Mensaje inválido recibido: {v} – {e}")
        return None

# Usa las variables de config
consumer = KafkaConsumer(
    'FallaCadenaDeFrio',
    'InventarioActualizado',
    bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=config.KAFKA_GROUP_ID,
    value_deserializer=safe_json_deserializer
)
producer = KafkaProducer(
    bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
print("🟢 Esperando mensajes...\n")

# Procesar mensajes según el tópico

for message in consumer:
    if message.value is None:
        continue
    try:
        if message.topic == "FallaCadenaDeFrio":
            print("📩 Mensaje recibido en FallaCadenaDeFrio:", message.value)

            # Validar existencia de clave productID
            if 'productID' not in message.value:
                print(f"⚠️ Mensaje ignorado: falta 'productID' en {message.value}")
                continue

            product_id = message.value['productID']

            # Paso 1: Buscar el producto
            tableProduct = dynamodb.Table('Producto')
            response = tableProduct.scan(
                FilterExpression='productID = :pid',
                ExpressionAttributeValues={':pid': str(product_id)}
            )
            items = response.get('Items', [])
            if not items:
                print(f"❌ Producto con ID {product_id} no encontrado")
                continue

            producto = items[0]
            nuevo_stock = producto['StockActual'] - Decimal('1')
            if nuevo_stock < 5:
                evento = {
                    "productID": producto['productID'],
                    "motivo": "Stock bajo después de FallaCadenaDeFrio"
                }
                producer.send('StockBajo', value=evento)
                print(f"✅ Evento enviado a Kafka: {evento}")

            if nuevo_stock < 0:
                print("⚠️ El stock no puede ser negativo")
                continue

            # Actualizar StockActual
            tableProduct.update_item(
                Key={'productID': producto['productID']},
                UpdateExpression='SET StockActual = :val',
                ExpressionAttributeValues={':val': nuevo_stock}
            )
            print(f"🔄 Stock actualizado para {producto['nombre']} a {nuevo_stock}")

            # Crear movimiento
            tableMov = dynamodb.Table('MovimientoInventario')
            response = tableMov.scan()
            items = response.get('Items', [])
            nuevo_num = max(
                (int(item['movimientoID'][3:]) for item in items if item['movimientoID'].startswith('MOV')),
                default=0
            ) + 1
            nuevo_movimientoID = f"MOV{nuevo_num:03d}"

            movimiento = {
                'movimientoID': nuevo_movimientoID,
                'tipoMovimiento': 'Fallo Cadena de Frio',
                'motivo': 'Descuento por Falla',
                'cantidad': Decimal('1'),
                'fechaMovimiento': str(datetime.now()),
                'usuarioResponsable': 'Sistema',
                'productoID': producto['productID']
            }
            tableMov.put_item(Item=movimiento)
            print(f"✅ Movimiento registrado: {movimiento}")

        elif message.topic == "InventarioActualizado":
            print(f"📦 Inventario actualizado: {message.value}")

            # Validar existencia de claves
            if 'productID' not in message.value or 'stock' not in message.value:
                print(f"⚠️ Mensaje ignorado: faltan campos en {message.value}")
                continue

            product_id = message.value['productID']
            nuevo_stock = Decimal(str(message.value['stock']))

            tableProduct = dynamodb.Table('Producto')
            response = tableProduct.scan(
                FilterExpression='productID = :pid',
                ExpressionAttributeValues={':pid': str(product_id)}
            )
            items = response.get('Items', [])
            if not items:
                print(f"❌ Producto con ID {product_id} no encontrado")
                continue

            producto = items[0]
            tableProduct.update_item(
                Key={'productID': producto['productID']},
                UpdateExpression='SET StockActual = :val',
                ExpressionAttributeValues={':val': nuevo_stock}
            )
            print(f"🔄 Stock actualizado para {producto['nombre']} a {nuevo_stock}")

            tableMov = dynamodb.Table('MovimientoInventario')
            response = tableMov.scan()
            items = response.get('Items', [])
            nuevo_num = max(
                (int(item['movimientoID'][3:]) for item in items if item['movimientoID'].startswith('MOV')),
                default=0
            ) + 1
            nuevo_movimientoID = f"MOV{nuevo_num:03d}"

            movimiento = {
                'movimientoID': nuevo_movimientoID,
                'tipoMovimiento': 'Actualización Inventario',
                'motivo': 'Actualización manual/external',
                'cantidad': nuevo_stock,
                'fechaMovimiento': str(datetime.now()),
                'usuarioResponsable': 'Sistema',
                'productoID': producto['productID']
            }
            tableMov.put_item(Item=movimiento)
            print(f"✅ Movimiento registrado: {movimiento}")

    except Exception as e:
        print(f"🚨 Error procesando mensaje: {message.value} -> {e}")
        continue  # Saltar al siguiente mensaje

producer.flush()
producer.close()

