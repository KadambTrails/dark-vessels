import asyncio
import json
import os
import logging
from aiokafka import AIOKafkaProducer
import websockets
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

load_dotenv()

logging.basicConfig(level=logging.INFO)

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Configuration
KAFKA_BROKER = 'localhost:19092'
KAFKA_TOPIC = 'maritime-ais-firehose'
AIS_STREAM_URL = "wss://stream.aisstream.io/v0/stream"
API_KEY = os.getenv("AIS_API_KEY")


def _blocking_db_fetch():
    """Synchronous database call executed inside a background thread"""
    connection = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        cursor_factory=RealDictCursor
    )
    try:
        with connection.cursor() as cursor:
            # Query the bounding box using PostGIS functions to get min/max coordinates
            query = """
                SELECT 
                    region_name AS name,
                    ST_XMin(geom) AS min_lon,
                    ST_YMin(geom) AS min_lat,
                    ST_XMax(geom) AS max_lon,
                    ST_YMax(geom) AS max_lat
                FROM monitoring_regions
                WHERE is_active = TRUE;
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            
            # Format rows into the specific structure required by aisstream.io
            formatted_regions = []
            for row in rows:
                formatted_regions.append({
                    "name": row["name"],
                    "box": [[[row["min_lon"], row["min_lat"]], [row["max_lon"], row["max_lat"]]]]
                })
            return formatted_regions
    finally:
        connection.close()

async def fetch_active_regions():
    """Asynchronous wrapper that prevents the DB query from blocking the event loop"""
    return await asyncio.to_thread(_blocking_db_fetch)

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
                    
                    raw_data["meta_region_source"] = region_name
                    
                    
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