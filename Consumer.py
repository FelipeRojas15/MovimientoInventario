from kafka import KafkaConsumer, KafkaProducer
import json
import boto3
import uuid
from datetime import datetime
from decimal import Decimal
import threading
import config
from fastapi import FastAPI, Request
import uvicorn


dynamodb = boto3.resource('dynamodb', **config.AWS_CONFIG)

consumer = KafkaConsumer(
    'FallaCadenaDeFrio',
    'InventarioActualizado',
    bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=config.KAFKA_GROUP_ID,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')) if v else None
)

producer = KafkaProducer(
    bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def kafka_listener():
    print("🟢 Esperando mensajes de Kafka...\n")
    for message in consumer:
        if message.value is None:
            continue
        try:
            if message.topic == "FallaCadenaDeFrio":
                procesar_falla_cadena(message.value)
            elif message.topic == "InventarioActualizado":
                procesar_inventario_actualizado(message.value)
        except Exception as e:
            print(f"🚨 Error procesando mensaje: {message.value} -> {e}")
            continue


def procesar_falla_cadena(data):
    print("📩 Mensaje recibido en FallaCadenaDeFrio:", data)

    if 'productID' not in data:
        print(f"⚠️ Mensaje ignorado: falta 'productID'")
        return

    tableProduct = dynamodb.Table('Producto')
    response = tableProduct.scan(
        FilterExpression='productID = :pid',
        ExpressionAttributeValues={':pid': str(data['productID'])}
    )
    items = response.get('Items', [])
    if not items:
        print(f"❌ Producto con ID {data['productID']} no encontrado")
        return

    producto = items[0]
    nuevo_stock = producto['StockActual'] - Decimal('1')

    if nuevo_stock < 5:
        evento = {"productID": producto['productID'], "motivo": "Stock bajo después de FallaCadenaDeFrio"}
        producer.send('StockBajo', value=evento)
        print(f"✅ Evento enviado a Kafka: {evento}")

    if nuevo_stock < 0:
        print("⚠️ El stock no puede ser negativo")
        return

    tableProduct.update_item(
        Key={'productID': producto['productID']},
        UpdateExpression='SET StockActual = :val',
        ExpressionAttributeValues={':val': nuevo_stock}
    )
    print(f"🔄 Stock actualizado para {producto['nombre']} a {nuevo_stock}")

    registrar_movimiento('Fallo Cadena de Frio', 'Descuento por Falla', Decimal('1'), producto['productID'])


def procesar_inventario_actualizado(data):
    print(f"📦 Inventario actualizado: {data}")
    if 'productID' not in data or 'stock' not in data:
        print(f"⚠️ Mensaje ignorado: faltan campos")
        return

    tableProduct = dynamodb.Table('Producto')
    response = tableProduct.scan(
        FilterExpression='productID = :pid',
        ExpressionAttributeValues={':pid': str(data['productID'])}
    )
    items = response.get('Items', [])
    if not items:
        print(f"❌ Producto con ID {data['productID']} no encontrado")
        return

    producto = items[0]
    nuevo_stock = Decimal(str(data['stock']))

    tableProduct.update_item(
        Key={'productID': producto['productID']},
        UpdateExpression='SET StockActual = :val',
        ExpressionAttributeValues={':val': nuevo_stock}
    )
    print(f"🔄 Stock actualizado para {producto['nombre']} a {nuevo_stock}")

    registrar_movimiento('Actualización Inventario', 'Actualización manual/external', nuevo_stock, producto['productID'])


def registrar_movimiento(tipo, motivo, cantidad, productoID):
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
        'tipoMovimiento': tipo,
        'motivo': motivo,
        'cantidad': cantidad,
        'fechaMovimiento': str(datetime.now()),
        'usuarioResponsable': 'Sistema',
        'productoID': productoID
    }
    tableMov.put_item(Item=movimiento)
    print(f"✅ Movimiento registrado: {movimiento}")


app = FastAPI()

@app.post("/api/v1/transaccion/registrar")
async def registrar_transaccion(request: Request):
    body = await request.json()
    required = ["tipoEvento", "idProducto", "datosEvento", "actorEmisor"]
    for campo in required:
        if campo not in body:
            return {"error": f"Falta el campo '{campo}'"}

    transaccion_id = str(uuid.uuid4())
    item = {
        "transaccionID": transaccion_id,
        "tipoEvento": body["tipoEvento"],
        "idProducto": body["idProducto"],
        "datosEvento": body["datosEvento"],
        "actorEmisor": body["actorEmisor"],
        "fechaRegistro": str(datetime.now())
    }

    table = dynamodb.Table('Transacciones')
    table.put_item(Item=item)
    print(f"✅ Transacción registrada: {item}")

    return {"message": "Transacción registrada exitosamente", "transaccionID": transaccion_id}



if __name__ == "__main__":
    t = threading.Thread(target=kafka_listener, daemon=True)
    t.start()
    uvicorn.run(app, host=config.SERVER_CONFIG['HOST'], port=config.SERVER_CONFIG['PORT'])
