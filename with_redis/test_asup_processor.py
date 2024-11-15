import pytest
import fakeredis
import json
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import threading
from queue import Queue
import random

# Import your AsupProcessor code
from asup_processor import AsupIdGenerator, AsupProcessor, AsupData

class MockKafkaMessage:
    """Mock Kafka message for testing"""
    def __init__(self, value, error=None):
        self._value = value
        self._error = error

    def value(self):
        return self._value

    def error(self):
        return self._error

class MockKafkaProducer:
    """Mock Kafka producer for testing"""
    def __init__(self):
        self.messages = []
        self.callbacks = []

    def produce(self, topic, value, key=None, callback=None):
        self.messages.append((topic, key, value))
        if callback:
            callback(None, MockKafkaMessage(value))

    def poll(self, timeout):
        return 0

    def flush(self):
        pass

class TestAsupIdGenerator:
    """Unit tests for ASUP ID generation"""
    
    @pytest.fixture
    def redis_client(self):
        return fakeredis.FakeRedis()

    @pytest.fixture
    def id_generator(self, redis_client):
        return AsupIdGenerator(redis_client)

    def test_basic_id_generation(self, id_generator):
        """Test basic ID generation"""
        timestamp = datetime.now()
        asup_id = id_generator.generate_id(timestamp)
        
        assert len(asup_id) == 16
        assert asup_id[:12] == timestamp.strftime("%Y%m%d%H%M")
        assert 0 <= int(asup_id[-4:]) <= 9999

    def test_sequence_increment(self, id_generator):
        """Test that sequence numbers increment correctly"""
        timestamp = datetime.now()
        id1 = id_generator.generate_id(timestamp)
        id2 = id_generator.generate_id(timestamp)
        
        assert int(id2[-4:]) == int(id1[-4:]) + 1

    def test_minute_overflow(self, id_generator):
        """Test handling of sequence overflow within a minute"""
        timestamp = datetime.now()
        # Simulate reaching max sequence
        minute_key = timestamp.strftime("%Y%m%d%H%M")
        id_generator.redis.set(f"{id_generator.COUNTER_KEY_PREFIX}{minute_key}", 9999)
        
        # Next ID should use a different minute
        new_id = id_generator.generate_id(timestamp)
        assert new_id is not None
        assert new_id[:12] != minute_key

    @pytest.mark.asyncio
    async def test_concurrent_id_generation(self, id_generator):
        """Test concurrent ID generation"""
        timestamp = datetime.now()
        generated_ids = set()
        num_concurrent = 100

        async def generate_id():
            asup_id = id_generator.generate_id(timestamp)
            generated_ids.add(asup_id)

        # Generate IDs concurrently
        tasks = [generate_id() for _ in range(num_concurrent)]
        await asyncio.gather(*tasks)

        assert len(generated_ids) == num_concurrent  # All IDs should be unique

class TestLoadScenarios:
    """Load testing scenarios"""
    
    @pytest.fixture
    def redis_client(self):
        return fakeredis.FakeRedis()

    def simulate_asup_spike(self, num_asups, duration_seconds, redis_client):
        """
        Simulate a spike of ASUP messages
        num_asups: Total number of ASUPs to generate
        duration_seconds: Duration over which to spread the ASUPs
        """
        id_generator = AsupIdGenerator(redis_client)
        results = Queue()
        
        def generate_asups(count):
            local_ids = []
            for _ in range(count):
                timestamp = datetime.now()
                asup_id = id_generator.generate_id(timestamp)
                if asup_id:
                    local_ids.append(asup_id)
                time.sleep(duration_seconds / num_asups)  # Distribute over duration
            results.put(local_ids)

        # Split work across threads
        num_threads = min(10, num_asups)  # Max 10 threads
        asups_per_thread = num_asups // num_threads
        
        threads = []
        for _ in range(num_threads):
            t = threading.Thread(target=generate_asups, args=(asups_per_thread,))
            threads.append(t)
            t.start()

        # Wait for all threads to complete
        for t in threads:
            t.join()

        # Collect all generated IDs
        all_ids = []
        while not results.empty():
            all_ids.extend(results.get())

        return all_ids

    def test_high_load_scenario(self, redis_client):
        """Test handling of high load (>10k ASUPs per minute)"""
        num_asups = 15000  # More than 10k
        duration_seconds = 60  # One minute
        
        generated_ids = self.simulate_asup_spike(num_asups, duration_seconds, redis_client)
        
        # Verify results
        assert len(generated_ids) == num_asups  # All ASUPs got IDs
        assert len(set(generated_ids)) == num_asups  # All IDs are unique
        
        # Check minute distribution
        minute_counts = {}
        for asup_id in generated_ids:
            minute = asup_id[:12]
            minute_counts[minute] = minute_counts.get(minute, 0) + 1
            assert minute_counts[minute] <= 10000  # No minute should exceed 9999

    def test_burst_scenario(self, redis_client):
        """Test handling of sudden bursts of ASUPs"""
        bursts = [
            (5000, 10),   # 5k ASUPs in 10 seconds
            (8000, 20),   # 8k ASUPs in 20 seconds
            (12000, 30),  # 12k ASUPs in 30 seconds
        ]
        
        all_ids = set()
        for num_asups, duration in bursts:
            burst_ids = self.simulate_asup_spike(num_asups, duration, redis_client)
            
            # Verify burst results
            assert len(burst_ids) == num_asups
            assert len(set(burst_ids).intersection(all_ids)) == 0  # No duplicates
            all_ids.update(burst_ids)

def test_end_to_end():
    """End-to-end test of the ASUP processing pipeline"""
    redis_client = fakeredis.FakeRedis()
    mock_producer = MockKafkaProducer()
    
    # Create test messages
    test_messages = [
        MockKafkaMessage(json.dumps({
            "data": f"test_data_{i}",
            "timestamp": datetime.now().isoformat()
        }).encode('utf-8'))
        for i in range(100)
    ]
    
    # Process messages
    processor = AsupProcessor(
        kafka_bootstrap_servers='dummy',
        input_topic='test-input',
        output_topic='test-output',
        redis_host='dummy',
        redis_port=6379,
        consumer_group='test-group'
    )
    processor.producer = mock_producer
    
    for msg in test_messages:
        result = processor.process_message(msg)
        assert result is not None
        asup_id, processed_data = result
        assert len(asup_id) == 16
        assert 'asup_id' in processed_data

if __name__ == "__main__":
    # Run a quick load test
    redis_client = fakeredis.FakeRedis()
    load_tester = TestLoadScenarios()
    
    print("Running load test simulation...")
    start_time = time.time()
    ids = load_tester.simulate_asup_spike(15000, 60, redis_client)
    end_time = time.time()
    
    print(f"Generated {len(ids)} unique IDs in {end_time - start_time:.2f} seconds")
    print(f"Number of unique minutes used: {len(set(id[:12] for id in ids))}")