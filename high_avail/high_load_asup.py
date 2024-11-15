import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
import json
import threading
from redis.cluster import RedisCluster
from redis.exceptions import RedisError
from kafka import KafkaConsumer, KafkaProducer
import boto3
from botocore.exceptions import ClientError
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor
import os
from prometheus_client import start_http_server, Counter, Gauge, Histogram
import hashlib

# Metrics for monitoring
PROCESSED_EVENTS = Counter('asup_processed_events_total', 'Total ASUP events processed')
FAILED_EVENTS = Counter('asup_failed_events_total', 'Total ASUP events that failed processing')
PROCESSING_TIME = Histogram('asup_processing_seconds', 'Time spent processing ASUP events')
CURRENT_MINUTE_COUNTER = Gauge('asup_current_minute_counter', 'Current minute counter value')
ID_GENERATION_LATENCY = Histogram('asup_id_generation_seconds', 'Time spent generating ASUP IDs')

@dataclass
class AsupEvent:
    """Represents an ASUP event"""
    raw_data: Dict[str, Any]
    timestamp: datetime
    source_system: str
    security_attributes: Dict[str, Any]
    asup_id: Optional[str] = None
    s3_path: Optional[str] = None

class HighLoadAsupProcessor:
    """
    Main processor handling high-load ASUP processing
    """
    def __init__(self, 
                 redis_nodes: List[Dict[str, Any]],
                 kafka_bootstrap_servers: List[str],
                 s3_bucket: str,
                 input_topic: str,
                 output_topic: str,
                 consumer_group: str,
                 max_workers: int = 10):
        """
        Initialize the ASUP processor with all necessary components
        """
        self.setup_logging()
        self.should_run = True
        self.max_workers = max_workers
        
        # Initialize components
        self.redis_cluster = self.setup_redis_cluster(redis_nodes)
        self.s3_client = boto3.client('s3')
        self.s3_bucket = s3_bucket
        self.input_topic = input_topic
        self.output_topic = output_topic
        
        # Initialize Kafka components
        self.consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=kafka_bootstrap_servers,
            group_id=consumer_group,
            enable_auto_commit=False,
            auto_offset_reset='earliest',
            max_poll_records=500,  # Batch size for efficiency
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            retries=5,
            acks='all',
            compression_type='gzip',
            batch_size=16384,
            linger_ms=50
        )
        
        # Thread pool for parallel processing
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        
        # Setup signal handlers
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        signal.signal(signal.SIGINT, self.handle_shutdown)

    def setup_logging(self):
        """Configure logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def setup_redis_cluster(self, redis_nodes: List[Dict[str, Any]]) -> RedisCluster:
        """Initialize Redis cluster connection with retry logic"""
        max_retries = 5
        for attempt in range(max_retries):
            try:
                return RedisCluster(
                    startup_nodes=redis_nodes,
                    decode_responses=True,
                    skip_full_coverage_check=True,
                    retry_on_timeout=True,
                    read_from_replicas=True
                )
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                self.logger.warning(f"Redis connection attempt {attempt + 1} failed: {e}")
                time.sleep(2 ** attempt)  # Exponential backoff

    def generate_id(self, timestamp: datetime) -> Optional[str]:
        """
        Generate unique ASUP ID with high-load handling
        """
        with ID_GENERATION_LATENCY.time():
            # Try current minute first
            current_minute = timestamp.strftime("%Y%m%d%H%M")
            id = self._try_generate_for_minute(current_minute)
            if id:
                return id

            # If current minute is full, try adjacent minutes
            for offset in range(-10, 11):
                if offset == 0:
                    continue
                    
                adjusted_time = timestamp + timedelta(minutes=offset)
                adjusted_minute = adjusted_time.strftime("%Y%m%d%H%M")
                
                id = self._try_generate_for_minute(adjusted_minute)
                if id:
                    return id
            
            return None

    def _try_generate_for_minute(self, minute: str) -> Optional[str]:
        """
        Attempt to generate ID for specific minute with Redis cluster
        """
        try:
            # Use Redis pipeline for efficiency
            pipeline = self.redis_cluster.pipeline()
            counter_key = f"asup_counter:{minute}"
            
            # Watch the counter key for changes
            pipeline.watch(counter_key)
            
            # Get current counter value
            current = pipeline.get(counter_key)
            current = int(current) if current else 0
            
            # Check if minute is full
            if current >= 9999:
                pipeline.unwatch()
                return None
            
            # Increment counter
            pipeline.multi()
            pipeline.incr(counter_key)
            pipeline.expire(counter_key, 86400)  # Expire after 24 hours
            result = pipeline.execute()
            
            if result:
                new_counter = result[0]
                if new_counter <= 9999:
                    CURRENT_MINUTE_COUNTER.set(new_counter)
                    return f"{minute}{new_counter:04d}"
            
            return None
            
        except RedisError as e:
            self.logger.error(f"Redis error during ID generation: {e}")
            return None

    def process_event(self, event_data: bytes) -> Optional[AsupEvent]:
        """
        Process a single ASUP event
        """
        try:
            with PROCESSING_TIME.time():
                # Parse event data
                raw_event = json.loads(event_data.decode('utf-8'))
                
                # Create event object
                event = AsupEvent(
                    raw_data=raw_event,
                    timestamp=datetime.now(),
                    source_system=raw_event.get('source', 'unknown'),
                    security_attributes=raw_event.get('security', {})
                )
                
                # Generate ASUP ID
                event.asup_id = self.generate_id(event.timestamp)
                if not event.asup_id:
                    self.logger.error("Failed to generate ASUP ID")
                    FAILED_EVENTS.inc()
                    return None
                
                # Store in S3
                event.s3_path = self._store_in_s3(event)
                if not event.s3_path:
                    self.logger.error("Failed to store in S3")
                    FAILED_EVENTS.inc()
                    return None
                
                # Publish processed event
                self._publish_event(event)
                
                PROCESSED_EVENTS.inc()
                return event
                
        except Exception as e:
            self.logger.error(f"Event processing failed: {e}")
            FAILED_EVENTS.inc()
            return None

    def _store_in_s3(self, event: AsupEvent) -> Optional[str]:
        """Store ASUP event in S3"""
        try:
            # Create S3 key using date-based partitioning
            date_prefix = event.timestamp.strftime('%Y/%m/%d')
            s3_key = f"asups/{date_prefix}/{event.asup_id}.json"
            
            # Prepare data for storage
            data = {
                'asup_id': event.asup_id,
                'timestamp': event.timestamp.isoformat(),
                'source_system': event.source_system,
                **event.raw_data
            }
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=json.dumps(data),
                ContentType='application/json'
            )
            
            return s3_key
            
        except ClientError as e:
            self.logger.error(f"S3 storage error: {e}")
            return None

    def _publish_event(self, event: AsupEvent):
        """Publish processed event to output topic"""
        try:
            output_data = {
                'asup_id': event.asup_id,
                'timestamp': event.timestamp.isoformat(),
                's3_path': event.s3_path,
                'source_system': event.source_system,
                **event.raw_data
            }
            
            # Publish with retry logic
            future = self.producer.send(
                self.output_topic,
                key=event.asup_id.encode('utf-8'),
                value=json.dumps(output_data).encode('utf-8')
            )
            
            # Wait for the message to be delivered
            future.get(timeout=10)
            
        except Exception as e:
            self.logger.error(f"Failed to publish event: {e}")
            raise

    def process_batch(self, messages: List[Any]):
        """Process a batch of messages in parallel"""
        futures = []
        
        for message in messages:
            future = self.executor.submit(self.process_event, message.value)
            futures.append((message, future))
        
        # Wait for all processing to complete
        processed = []
        for message, future in futures:
            try:
                result = future.result(timeout=30)
                if result:
                    processed.append(message)
            except Exception as e:
                self.logger.error(f"Batch processing error: {e}")
        
        # Commit offsets for successfully processed messages
        if processed:
            self.consumer.commit()

    def run(self):
        """Main processing loop"""
        self.logger.info("Starting ASUP processor...")
        
        # Start Prometheus metrics server
        start_http_server(8000)
        
        try:
            while self.should_run:
                messages = self.consumer.poll(timeout_ms=1000)
                
                for tp, msgs in messages.items():
                    if msgs:
                        self.process_batch(msgs)
                        
        except Exception as e:
            self.logger.error(f"Processing error: {e}")
        finally:
            self.cleanup()

    def handle_shutdown(self, signum, frame):
        """Handle graceful shutdown"""
        self.logger.info("Initiating graceful shutdown...")
        self.should_run = False

    def cleanup(self):
        """Cleanup resources"""
        self.logger.info("Cleaning up resources...")
        self.executor.shutdown(wait=True)
        self.consumer.close()
        self.producer.flush()
        self.producer.close()
        self.redis_cluster.close()

if __name__ == "__main__":
    # Configuration from environment variables
    config = {
        'redis_nodes': [
            {'host': os.getenv('REDIS_HOST_1', 'localhost'), 'port': 6379},
            {'host': os.getenv('REDIS_HOST_2', 'localhost'), 'port': 6379},
            {'host': os.getenv('REDIS_HOST_3', 'localhost'), 'port': 6379}
        ],
        'kafka_bootstrap_servers': os.getenv('KAFKA_SERVERS', 'localhost:9092').split(','),
        's3_bucket': os.getenv('S3_BUCKET', 'asup-storage'),
        'input_topic': os.getenv('INPUT_TOPIC', 'asup-raw-events'),
        'output_topic': os.getenv('OUTPUT_TOPIC', 'asup-processed-events'),
        'consumer_group': os.getenv('CONSUMER_GROUP', 'asup-processor-group'),
        'max_workers': int(os.getenv('MAX_WORKERS', '10'))
    }
    
    # Start processor
    processor = HighLoadAsupProcessor(**config)
    processor.run()