import asyncio
import json
import os
import logging
import gzip
import io
from aiokafka import AIOKafkaProducer
import websockets
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

# Load environment configurations
load_dotenv()

# Setup logging architecture
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Database infrastructure parameters
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 5432)) 
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Message Broker & Stream configurations
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
KAFKA_TOPIC = 'maritime-ais-firehose'
AIS_STREAM_URL = "wss://stream.aisstream.io/v0/stream"
API_KEY = os.getenv("AIS_API_KEY")


def _blocking_db_fetch():
    """Synchronous database call executed inside an isolated background thread"""
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
            # Query the bounding box using PostGIS envelope parsing functions
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
            
            # Formats coordinates to match exact [Longitude, Latitude] schema requirements
            formatted_regions = []
            for row in rows:
                formatted_regions.append({
                    "name": row["name"],
                    "box": [[[row["min_lat"], row["min_lon"]], [row["max_lat"], row["max_lon"]]]]
                    })
            return formatted_regions
    finally:
        connection.close()

async def fetch_active_regions():
    """Asynchronous wrapper preventing the database driver from blocking the event loop"""
    return await asyncio.to_thread(_blocking_db_fetch)

async def stream_region_worker(region_name, bounding_boxes, producer):
    """Isolated worker task managing a single region's socket lifecycle, frame decoding, and streaming"""
    message_count = 0
    while True:
        try:
            logging.info(f"Connecting WebSocket for region: {region_name}...")
            async with websockets.connect(AIS_STREAM_URL) as ws:
                
                # Send explicit authorization envelope alongside target spatial bounding boxes
                subscribe_msg = {
                    "APIKey": API_KEY,
                    "BoundingBoxes": bounding_boxes
                }
                
                await ws.send(json.dumps(subscribe_msg))

                logging.info(f" [✓] Subscription successfully verified for {region_name}")

                while True:
                    try:
                        # Enforce a 30s read timeout acting as an infrastructure heartbeat
                        message = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        
                        # Handle Text frames vs Compressed Gzip Binary data payloads seamlessly
                        if isinstance(message, bytes):
                            try:
                                decoded_message = message.decode('utf-8')
                            except UnicodeDecodeError:
                                with gzip.GzipFile(fileobj=io.BytesIO(message)) as f:
                                    decoded_message = f.read().decode('utf-8')
                        else:
                            decoded_message = message

                        raw_data = json.loads(decoded_message)
                        
                        # Extract message identifier metadata
                        mmsi = raw_data.get("MetaData", {}).get("MMSI")
                        message_count += 1
                        
                        # Log intervals to confirm stream velocity without flooding stdout
                        if message_count % 10 == 0 or message_count == 1:
                            logging.info(f" [{region_name}] Ingested message #{message_count} (MMSI: {mmsi})")

                        # Append tracking regional metadata origin to payload
                        raw_data["meta_region_source"] = region_name
                        
                        # Extract the key to guarantee message preservation order within Redpanda partitions
                        mmsi_key = str(mmsi or "unknown").encode('utf-8')
                        
                        await producer.send_and_wait(
                            KAFKA_TOPIC, 
                            key=mmsi_key,
                            value=json.dumps(raw_data).encode('utf-8')
                        )
                        
                    except asyncio.TimeoutError:
                        logging.info(f" [Heartbeat] {region_name} connection stable. Awaiting live vessel data...")
                        continue
                    except Exception as parse_error:
                        logging.error(f" [{region_name}] Internal extraction error encountered: {parse_error}")
                        continue
                        
        except Exception as e:
            # Extended cool-down loop spacing out connection attempts if an unexpected crash or network drop hits
            logging.error(f"Worker {region_name} encountered an error: {e}. Cooling down. Reconnecting in 15s...")
            await asyncio.sleep(15)

async def delayed_worker_start(delay, region_name, bounding_boxes, producer):
    """Staggers worker thread execution to explicitly prevent concurrent HTTP 429 API rate limits"""
    if delay > 0:
        logging.info(f"Staggering pipeline activation. {region_name} holding for {delay}s before handshaking...")
        await asyncio.sleep(delay)
    await stream_region_worker(region_name, bounding_boxes, producer)

async def main():
    # Constructing async Kafka event producer
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)
    await producer.start()
    
    try:
        # Load geographic constraints dynamically from PostGIS
        regions = await fetch_active_regions()
        
        # Build worker stack with progressive delay calculations
        tasks = []
        for i, r in enumerate(regions):
            # Creates an explicit 2.5-second buffer gap between connection handshakes
            start_delay = i * 2.5
            tasks.append(
                delayed_worker_start(start_delay, r["name"], r["box"], producer)
            )
        
        # Drive concurrent streaming threads
        await asyncio.gather(*tasks)
    finally:
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(main())