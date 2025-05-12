import json
import boto3
import time
from decimal import Decimal # Decimal tipini kullanmak için import ediyoruz

# DynamoDB kaynağını başlat
dynamodb = boto3.resource('dynamodb')
# Tablo adını buraya girin (DynamoDB'de oluşturduğunuz adla aynı olmalı)
TABLE_NAME = "SensorData" # DynamoDB tablo adınızla eşleştiğinden emin olun
table = dynamodb.Table(TABLE_NAME)

def convert_float_to_decimal(item):
    """
    Recursively converts float values in a dictionary (or list of dictionaries) to Decimal.
    """
    if isinstance(item, dict):
        return {k: convert_float_to_decimal(v) for k, v in item.items()}
    elif isinstance(item, list):
        return [convert_float_to_decimal(i) for i in item]
    elif isinstance(item, float):
        # Precision'ı korumak için string üzerinden Decimal'e çevirme
        return Decimal(str(item)) 
    return item

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))

    try:
        # Gelen olaydaki float'ları Decimal'e çevir
        item_to_store = convert_float_to_decimal(event)
        
        print("Item to store (after Decimal conversion): " + json.dumps(item_to_store, default=str, indent=2)) # Decimal'i loglamak için default=str

        # Veriyi DynamoDB'ye yaz
        table.put_item(
            Item=item_to_store
        )
        print(f"Successfully wrote item to DynamoDB: {item_to_store}")
        return {
            'statusCode': 200,
            'body': json.dumps(f'Successfully processed message and stored in {TABLE_NAME}')
        }
    except Exception as e:
        print(f"Error processing message: {e}")
        # Hata durumunda daha detaylı loglama veya hata işleme yapılabilir
        raise e
