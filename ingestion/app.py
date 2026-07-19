import asyncio
import json
import os
import logging
from aiokafka import AIOKafkaProducer
import websockets
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)

# Configuration
KAFKA_BROKER = 'localhost:19092'
KAFKA_TOPIC = 'maritime-ais-firehose'
AIS_STREAM_URL = "wss://stream.aisstream.io/v0/stream"
API_KEY = os.getenv("AIS_API_KEY")

# MOCK DATABASE FETCH: Replace this function with a real 'psycopg2' query later!
async def fetch_active_regions():
    return [
        {
            "name": "Strait_of_Malacca", 
            "box": [[[1.0, 95.0], [6.0, 104.5]]]
        },
        {
            "name": "English_Channel", 
            "box": [[[48.5, -6.0], [51.5, 2.0]]]
        },
    ]

async def stream_region_worker(region_name, bounding_boxes, producer):
    """Isolated worker task that manages a single region's socket connection"""
    while True:
        try:
            logging.info(f"Connecting WebSocket for region: {region_name}...")
            async with websockets.connect(AIS_STREAM_URL) as ws:
                # 1. Send authentication & custom bounding box payload
                subscribe_msg = {
                    "APIKey": API_KEY,
                    "BoundingBoxes": bounding_boxes
                }
                await ws.send(json.dumps(subscribe_msg))
                logging.info(f" [✓] Subscription active for {region_name}")

                # 2. Listen continuously to the incoming stream
                async for message in ws:
                    raw_data = json.loads(message)
                    
                    # ENRICHMENT: Inject the region name into the payload metadata!
                    raw_data["meta_region_source"] = region_name
                    
                    # 3. Stream immediately to your single shared Kafka topic
                    await producer.send_and_wait(
                        KAFKA_TOPIC, 
                        json.dumps(raw_data).encode('utf-8')
                    )
                    
        except Exception as e:
            logging.error(f"Worker {region_name} encountered an error: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)

async def main():
    # Initialize high-performance async Kafka producer
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)
    await producer.start()
    
    try:
        # Load targets dynamically from database data
        regions = await fetch_active_regions()
        
        # Build worker threads concurrently
        tasks = []
        for r in regions:
            tasks.append(stream_region_worker(r["name"], r["box"], producer))
        
        # Run all workers in parallel indefinitely
        await asyncio.gather(*tasks)
    finally:
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(main())