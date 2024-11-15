from datetime import datetime, timedelta
import random
from typing import Optional, Tuple
from confluent_kafka import Consumer, Producer, KafkaError
import json
import threading
import logging
from dataclasses import dataclass
import redis
import time

@dataclass
class AsupData:
    """Data class to hold ASUP information"""
    raw_data: dict
    timestamp: datetime
    sequence_number: int

class AsupIdGenerator:
    """
    Handles ASUP ID generation with minute-level flexibility and Redis-based coordination
    """
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.COUNTER_KEY_PREFIX = "asup_counter:"
        self.COUNTER_LOCK_PREFIX = "asup_lock:"
        self.MAX_SEQUENCE = 9999
        self.MAX_MINUTE_ADJUST = 10

    def _get_next_sequence(self, minute_key: str) -> Optional[int]:
        """Get next sequence number with Redis atomic operations"""
        pipeline = self.redis.pipeline()
        try:
            # Increment counter for this minute
            current = self.redis.incr(f"{self.COUNTER_KEY_PREFIX}{minute_key}")
            
            if current > self.MAX_SEQUENCE:
                # Reset counter if we've exceeded max
                self.redis.delete(f"{self.COUNTER_KEY_PREFIX}{minute_key}")
                return None
                
            return current
        except Exception as e:
            logging.error(f"Redis operation failed: {e}")
            return None

    def generate_id(self, timestamp: datetime) -> Optional[str]:
        """
        Generate ASUP ID with minute-level flexibility when needed
        Returns: YYYYMMDDHHMM#### or None if generation fails
        """
        original_minute = timestamp
        
        # Try original minute first
        minute_key = original_minute.strftime("%Y%m%d%H%M")
        sequence = self._get_next_sequence(minute_key)
        
        if sequence is not None:
            return f"{minute_key}{sequence:04d}"

        # If original minute is full, try adjacent minutes
        for minute_offset in range(-self.MAX_MINUTE_ADJUST, self.MAX_MINUTE_ADJUST + 1):
            if minute_offset == 0:
                continue
                
            adjusted_time = original_minute + timedelta(minutes=minute_offset)
            minute_key = adjusted_time.strftime("%Y%m%d%H%M")
            sequence = self._get_next_sequence(minute_key)
            
            if sequence is not None:
                return f"{minute_key}{sequence:04d}"
        
        logging.error("Failed to generate ASUP ID - all minute slots full")
        return None

class AsupProcessor:
    """
    Main processor class that handles Kafka consumption, ID generation, and publishing
    """
    def __init__(self, 
                 kafka_bootstrap_servers: str,
                 input_topic: str,
                 output_topic: str,
                 redis_host: str,
                 redis_port: int,
                 consumer_group: str):
        
        self.input_topic = input_topic
        self.output_topic = output_topic
        
        # Initialize Kafka consumer
        self.consumer = Consumer({
            'bootstrap.servers': kafka_bootstrap_servers,
            'group.id': consumer_group,
            'auto.offset.reset': 'earliest'
        })
        
        # Initialize Kafka producer
        self.producer = Producer({
            'bootstrap.servers': kafka_bootstrap_servers
        })
        
        # Initialize Redis and ID generator
        redis_client = redis.Redis(host=redis_host, port=redis_port)
        self.id_generator = AsupIdGenerator(redis_client)
        
        self.running = False
        
    def delivery_report(self, err, msg):
        """Kafka producer delivery callback"""
        if err is not None:
            logging.error(f'Message delivery failed: {err}')
        else:
            logging.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def process_message(self, msg) -> Optional[Tuple[str, dict]]:
        """Process a single message and generate ASUP ID"""
        try:
            data = json.loads(msg.value().decode('utf-8'))
            timestamp = datetime.now()
            
            asup_id = self.id_generator.generate_id(timestamp)
            if not asup_id:
                logging.error("Failed to generate ASUP ID")
                return None
                
            # Add ASUP ID to data
            data['asup_id'] = asup_id
            return asup_id, data
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            return None

    def run(self):
        """Main processing loop"""
        self.running = True
        self.consumer.subscribe([self.input_topic])
        
        try:
            while self.running:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logging.debug(f'Reached end of partition {msg.partition()}')
                    else:
                        logging.error(f'Error while consuming: {msg.error()}')
                    continue
                
                result = self.process_message(msg)
                if result:
                    asup_id, processed_data = result
                    
                    # Publish to output topic
                    self.producer.produce(
                        self.output_topic,
                        key=asup_id,
                        value=json.dumps(processed_data).encode('utf-8'),
                        callback=self.delivery_report
                    )
                    self.producer.poll(0)  # Trigger delivery reports
                    
        except Exception as e:
            logging.error(f"Processing error: {e}")
        finally:
            self.consumer.close()
            self.producer.flush()

    def stop(self):
        """Stop the processor"""
        self.running = False

# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    processor = AsupProcessor(
        kafka_bootstrap_servers='localhost:9092',
        input_topic='asup-raw',
        output_topic='asup-processed',
        redis_host='localhost',
        redis_port=6379,
        consumer_group='asup-processor-group'
    )
    
    try:
        processor.run()
    except KeyboardInterrupt:
        processor.stop()