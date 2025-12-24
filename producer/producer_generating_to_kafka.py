import os
import json
import time
import uuid
import random
import signal
import sys
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer
from dotenv import load_dotenv

# ------------------------------------------------------------------------------
# Load env
# ------------------------------------------------------------------------------
load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "spotify-events")
USER_COUNT = int(os.getenv("USER_COUNT", 20))
EVENT_INTERVAL_SECONDS = float(os.getenv("EVENT_INTERVAL_SECONDS", 1))

fake = Faker()

# ------------------------------------------------------------------------------
# Static data
# ------------------------------------------------------------------------------
song_artist_pairs = [
    {"artist": "The Weeknd", "song": "Blinding Lights"},
    {"artist": "Dua Lipa", "song": "Levitating"},
    {"artist": "Drake", "song": "God's Plan"},
    {"artist": "Taylor Swift", "song": "Love Story"},
    {"artist": "Ed Sheeran", "song": "Shape of You"},
    {"artist": "Kanye West", "song": "Stronger"},
]

for p in song_artist_pairs:
    p["song_id"] = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{p['artist']}::{p['song']}"))

devices = ["mobile", "desktop", "web"]
countries = ["US", "UK", "CA", "AU", "IN", "DE"]
event_types = ["play", "pause", "skip", "add_to_playlist"]

user_ids = [str(uuid.uuid4()) for _ in range(USER_COUNT)]

# ------------------------------------------------------------------------------
# Kafka Producer
# ------------------------------------------------------------------------------
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
)

running = True

# ------------------------------------------------------------------------------
# Pretty printing helpers
# ------------------------------------------------------------------------------
def pretty_event(event):
    return (
        f"🎵 EVENT | "
        f"{event['event_type']:<12} | "
        f"{event['song_name']:<18} | "
        f"{event['artist_name']:<14} | "
        f"user={event['user_id'][:6]} | "
        f"{event['country']} | "
        f"{event['device_type']}"
    )

def pretty_sent(meta):
    return (
        f"📦 SENT  | "
        f"topic={meta.topic} | "
        f"p={meta.partition} | "
        f"offset={meta.offset}"
    )

# ------------------------------------------------------------------------------
# Graceful shutdown
# ------------------------------------------------------------------------------
def shutdown(sig, frame):
    global running
    print("\n🛑 Stopping producer... flushing Kafka")
    running = False
    producer.flush()
    producer.close()
    print("✅ Producer closed")
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

# ------------------------------------------------------------------------------
# Event generator
# ------------------------------------------------------------------------------
def generate_event():
    pair = random.choice(song_artist_pairs)
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.choice(user_ids),
        "song_id": pair["song_id"],
        "artist_name": pair["artist"],
        "song_name": pair["song"],
        "event_type": random.choice(event_types),
        "device_type": random.choice(devices),
        "country": random.choice(countries),
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }

# ------------------------------------------------------------------------------
# Main loop
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    print("🎧 Spotify Kafka Producer started")
    print(f"➡ Broker: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"➡ Topic : {KAFKA_TOPIC}")
    print("-" * 80)

    while running:
        event = generate_event()

        future = producer.send(KAFKA_TOPIC, event)
        future.add_callback(lambda meta: print(pretty_sent(meta)))

        print(pretty_event(event))

        time.sleep(EVENT_INTERVAL_SECONDS)



# import os
# import json
# import time
# import uuid
# import random
# import logging
# import signal
# import sys
# from datetime import datetime
# from faker import Faker
# from kafka import KafkaProducer
# from kafka.errors import KafkaError
# from dotenv import load_dotenv

# # ------------------------------------------------------------------------------
# # Load environment variables
# # ------------------------------------------------------------------------------
# load_dotenv()

# KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
# KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "spotify-events")
# USER_COUNT = int(os.getenv("USER_COUNT", 20))
# EVENT_INTERVAL_SECONDS = float(os.getenv("EVENT_INTERVAL_SECONDS", 1))
# MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))

# # ------------------------------------------------------------------------------
# # Logging configuration
# # ------------------------------------------------------------------------------
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
# )
# logger = logging.getLogger("spotify-producer")

# # ------------------------------------------------------------------------------
# # Faker & static data
# # ------------------------------------------------------------------------------
# fake = Faker()

# song_artist_pairs = [
#     {"artist": "The Weeknd", "song": "Blinding Lights"},
#     {"artist": "Dua Lipa", "song": "Levitating"},
#     {"artist": "Drake", "song": "God's Plan"},
#     {"artist": "Taylor Swift", "song": "Love Story"},
#     {"artist": "Ed Sheeran", "song": "Shape of You"},
#     {"artist": "Kanye West", "song": "Stronger"},
# ]

# # Stable song_id generation
# for pair in song_artist_pairs:
#     pair["song_id"] = str(
#         uuid.uuid5(uuid.NAMESPACE_DNS, f"{pair['artist']}::{pair['song']}")
#     )

# devices = ["mobile", "desktop", "web"]
# countries = ["US", "UK", "CA", "AU", "IN", "DE"]
# event_types = ["play", "pause", "skip", "add_to_playlist"]

# user_ids = [str(uuid.uuid4()) for _ in range(USER_COUNT)]

# # ------------------------------------------------------------------------------
# # Kafka Producer
# # ------------------------------------------------------------------------------
# producer = KafkaProducer(
#     bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
#     value_serializer=lambda v: json.dumps(v).encode("utf-8"),
#     acks="all",                     # strongest durability
#     retries=MAX_RETRIES,
#     linger_ms=10,                   # batching
#     request_timeout_ms=30000,
# )

# running = True

# # ------------------------------------------------------------------------------
# # Graceful shutdown
# # ------------------------------------------------------------------------------
# def shutdown_handler(sig, frame):
#     global running
#     logger.info("🛑 Shutdown signal received. Flushing Kafka producer...")
#     running = False
#     producer.flush()
#     producer.close()
#     logger.info("✅ Producer closed gracefully.")
#     sys.exit(0)

# signal.signal(signal.SIGINT, shutdown_handler)
# signal.signal(signal.SIGTERM, shutdown_handler)

# # ------------------------------------------------------------------------------
# # Event generator
# # ------------------------------------------------------------------------------
# def generate_event():
#     pair = random.choice(song_artist_pairs)
#     return {
#         "event_id": str(uuid.uuid4()),
#         "user_id": random.choice(user_ids),
#         "song_id": pair["song_id"],
#         "artist_name": pair["artist"],
#         "song_name": pair["song"],
#         "event_type": random.choice(event_types),
#         "device_type": random.choice(devices),
#         "country": random.choice(countries),
#         "timestamp": datetime.utcnow().isoformat() + "Z",
#     }

# # ------------------------------------------------------------------------------
# # Delivery callback
# # ------------------------------------------------------------------------------
# def on_send_success(record_metadata):
#     logger.debug(
#         f"Delivered to {record_metadata.topic} "
#         f"[partition={record_metadata.partition}, offset={record_metadata.offset}]"
#     )

# def on_send_error(excp):
#     logger.error("❌ Failed to deliver message", exc_info=excp)

# # ------------------------------------------------------------------------------
# # Main loop
# # ------------------------------------------------------------------------------
# if __name__ == "__main__":
#     logger.info("🎧 Starting Spotify Kafka Producer")
#     logger.info(f"Kafka broker: {KAFKA_BOOTSTRAP_SERVERS}")
#     logger.info(f"Topic: {KAFKA_TOPIC}")
#     logger.info(f"Users: {USER_COUNT}")

#     while running:
#         try:
#             event = generate_event()

#             future = producer.send(KAFKA_TOPIC, event)
#             future.add_callback(on_send_success)
#             future.add_errback(on_send_error)

#             logger.info(
#                 f"Produced event | "
#                 f"type={event['event_type']} | "
#                 f"song={event['song_name']} | "
#                 f"user={event['user_id']}"
#             )

#             time.sleep(EVENT_INTERVAL_SECONDS)

#         except KafkaError as e:
#             logger.error("Kafka error occurred", exc_info=e)
#             time.sleep(2)

#         except Exception as e:
#             logger.exception("Unexpected error")
#             time.sleep(2)

# # import os
# # import json
# # import time
# # import uuid
# # import random
# # from faker import Faker
# # from datetime import datetime
# # from kafka import KafkaProducer
# # from dotenv import load_dotenv

# # # --------------------------------------------
# # # Load environment variables
# # # --------------------------------------------
# # load_dotenv()

# # KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
# # KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "spotify-events")
# # USER_COUNT = int(os.getenv("USER_COUNT", 20))
# # EVENT_INTERVAL_SECONDS = int(os.getenv("EVENT_INTERVAL_SECONDS", 1))

# # fake = Faker()

# # # Kafka Producer
# # producer = KafkaProducer(
# #     bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
# #     value_serializer=lambda v: json.dumps(v).encode("utf-8")
# # )

# # # --------------------------------------------
# # # Stable Song/Artist Definitions
# # # --------------------------------------------
# # song_artist_pairs = [
# #     {"artist": "The Weeknd", "song": "Blinding Lights"},
# #     {"artist": "Dua Lipa", "song": "Levitating"},
# #     {"artist": "Drake", "song": "God's Plan"},
# #     {"artist": "Taylor Swift", "song": "Love Story"},
# #     {"artist": "Ed Sheeran", "song": "Shape of You"},
# #     {"artist": "Kanye West", "song": "Stronger"}
# # ]

# # for pair in song_artist_pairs:
# #     name_for_uuid = f"{pair['artist']}::{pair['song']}"
# #     pair["song_id"] = str(uuid.uuid5(uuid.NAMESPACE_DNS, name_for_uuid))

# # devices = ["mobile", "desktop", "web"]
# # countries = ["US", "UK", "CA", "AU", "IN", "DE"]
# # event_types = ["play", "pause", "skip", "add_to_playlist"]

# # # Generate random users
# # user_ids = [str(uuid.uuid4()) for _ in range(USER_COUNT)]

# # def generate_event():
# #     pair = random.choice(song_artist_pairs)
# #     user_id = random.choice(user_ids)
# #     return {
# #         "event_id": str(uuid.uuid4()),
# #         "user_id": user_id,
# #         "song_id": pair["song_id"],
# #         "artist_name": pair["artist"],
# #         "song_name": pair["song"],
# #         "event_type": random.choice(event_types),
# #         "device_type": random.choice(devices),
# #         "country": random.choice(countries),
# #         "timestamp": datetime.utcnow().isoformat() + "Z"
# #     }

# # if __name__ == "__main__":
# #     print("🎧 Starting Spotify data simulator...")
# #     print(f"Using {len(song_artist_pairs)} songs and {len(user_ids)} users.")
# #     for p in song_artist_pairs:
# #         print(f"{p['song']} — {p['artist']} -> song_id={p['song_id']}")

# #     while True:
# #         event = generate_event()
# #         producer.send(KAFKA_TOPIC, event)
# #         print(f"Produced event: {event['event_type']} - {event['song_name']} by {event['artist_name']} (user {event['user_id']})")
# #         time.sleep(EVENT_INTERVAL_SECONDS)

        
# # # """
# # # Spotify Event Data Simulator
# # # Generates realistic Spotify streaming events and publishes them to Kafka.
# # # """
# # # import os
# # # import sys
# # # import json
# # # import time
# # # import uuid
# # # import random
# # # import logging
# # # from typing import Dict, List, Optional
# # # from datetime import datetime
# # # from dataclasses import dataclass, asdict

# # # from kafka import KafkaProducer
# # # from kafka.errors import KafkaError
# # # from faker import Faker
# # # from dotenv import load_dotenv


# # # # Configure logging
# # # logging.basicConfig(
# # #     level=logging.INFO,
# # #     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
# # #     handlers=[
# # #         logging.StreamHandler(sys.stdout),
# # #         logging.FileHandler('spotify_simulator.log')
# # #     ]
# # # )
# # # logger = logging.getLogger(__name__)


# # # @dataclass
# # # class SongArtistPair:
# # #     """Represents a song-artist combination with unique identifier."""
# # #     artist: str
# # #     song: str
# # #     song_id: str = None

# # #     def __post_init__(self):
# # #         if self.song_id is None:
# # #             name_for_uuid = f"{self.artist}::{self.song}"
# # #             self.song_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, name_for_uuid))


# # # @dataclass
# # # class SpotifyEvent:
# # #     """Represents a Spotify streaming event."""
# # #     event_id: str
# # #     user_id: str
# # #     song_id: str
# # #     artist_name: str
# # #     song_name: str
# # #     event_type: str
# # #     device_type: str
# # #     country: str
# # #     timestamp: str


# # # class Config:
# # #     """Application configuration loaded from environment variables."""
    
# # #     def __init__(self):
# # #         load_dotenv()
        
# # #         self.kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
# # #         self.kafka_topic = os.getenv("KAFKA_TOPIC", "spotify-events")
# # #         self.user_count = int(os.getenv("USER_COUNT", "20"))
# # #         self.event_interval_seconds = float(os.getenv("EVENT_INTERVAL_SECONDS", "1.0"))
# # #         self.max_retries = int(os.getenv("MAX_RETRIES", "3"))
        
# # #         self._validate()
    
# # #     def _validate(self):
# # #         """Validate configuration parameters."""
# # #         if self.user_count <= 0:
# # #             raise ValueError("USER_COUNT must be positive")
# # #         if self.event_interval_seconds < 0:
# # #             raise ValueError("EVENT_INTERVAL_SECONDS must be non-negative")
# # #         if not self.kafka_bootstrap_servers:
# # #             raise ValueError("KAFKA_BOOTSTRAP_SERVERS is required")


# # # class SpotifyEventGenerator:
# # #     """Generates realistic Spotify streaming events."""
    
# # #     SONG_ARTIST_PAIRS = [
# # #         ("The Weeknd", "Blinding Lights"),
# # #         ("Dua Lipa", "Levitating"),
# # #         ("Drake", "God's Plan"),
# # #         ("Taylor Swift", "Love Story"),
# # #         ("Ed Sheeran", "Shape of You"),
# # #         ("Kanye West", "Stronger"),
# # #         ("Billie Eilish", "Bad Guy"),
# # #         ("Post Malone", "Circles"),
# # #         ("Ariana Grande", "7 rings"),
# # #         ("Justin Bieber", "Peaches"),
# # #     ]
    
# # #     DEVICES = ["mobile", "desktop", "web", "tablet", "smart_speaker"]
# # #     COUNTRIES = ["US", "UK", "CA", "AU", "IN", "DE", "FR", "BR", "MX", "JP"]
# # #     EVENT_TYPES = ["play", "pause", "skip", "add_to_playlist", "like", "share"]
    
# # #     def __init__(self, user_count: int):
# # #         self.fake = Faker()
# # #         self.songs = [
# # #             SongArtistPair(artist=artist, song=song)
# # #             for artist, song in self.SONG_ARTIST_PAIRS
# # #         ]
# # #         self.user_ids = [str(uuid.uuid4()) for _ in range(user_count)]
# # #         logger.info(f"Initialized generator with {len(self.songs)} songs and {len(self.user_ids)} users")
    
# # #     def generate_event(self) -> SpotifyEvent:
# # #         """Generate a single random Spotify event."""
# # #         song = random.choice(self.songs)
        
# # #         return SpotifyEvent(
# # #             event_id=str(uuid.uuid4()),
# # #             user_id=random.choice(self.user_ids),
# # #             song_id=song.song_id,
# # #             artist_name=song.artist,
# # #             song_name=song.song,
# # #             event_type=random.choice(self.EVENT_TYPES),
# # #             device_type=random.choice(self.DEVICES),
# # #             country=random.choice(self.COUNTRIES),
# # #             timestamp=datetime.utcnow().isoformat() + "Z"
# # #         )
    
# # #     def get_songs_summary(self) -> List[Dict]:
# # #         """Get summary of all available songs."""
# # #         return [
# # #             {
# # #                 "song": song.song,
# # #                 "artist": song.artist,
# # #                 "song_id": song.song_id
# # #             }
# # #             for song in self.songs
# # #         ]


# # # class KafkaEventProducer:
# # #     """Handles Kafka producer operations with error handling and retry logic."""
    
# # #     def __init__(self, config: Config):
# # #         self.config = config
# # #         self.producer: Optional[KafkaProducer] = None
# # #         self._connect()
    
# # #     def _connect(self):
# # #         """Establish connection to Kafka broker."""
# # #         try:
# # #             self.producer = KafkaProducer(
# # #                 bootstrap_servers=self.config.kafka_bootstrap_servers.split(','),
# # #                 value_serializer=lambda v: json.dumps(v).encode("utf-8"),
# # #                 acks='all',  # Wait for all replicas to acknowledge
# # #                 retries=self.config.max_retries,
# # #                 max_in_flight_requests_per_connection=1,  # Ensure ordering
# # #                 compression_type='gzip'  # Compress messages
# # #             )
# # #             logger.info(f"Connected to Kafka at {self.config.kafka_bootstrap_servers}")
# # #         except KafkaError as e:
# # #             logger.error(f"Failed to connect to Kafka: {e}")
# # #             raise
    
# # #     def send_event(self, event: SpotifyEvent) -> bool:
# # #         """
# # #         Send event to Kafka topic.
        
# # #         Returns:
# # #             bool: True if successful, False otherwise
# # #         """
# # #         try:
# # #             event_dict = asdict(event)
# # #             future = self.producer.send(self.config.kafka_topic, event_dict)
            
# # #             # Block for 'synchronous' sends (optional, can be removed for async)
# # #             record_metadata = future.get(timeout=10)
            
# # #             logger.debug(
# # #                 f"Event sent: {event.event_type} - {event.song_name} by {event.artist_name} "
# # #                 f"(partition: {record_metadata.partition}, offset: {record_metadata.offset})"
# # #             )
# # #             return True
            
# # #         except KafkaError as e:
# # #             logger.error(f"Failed to send event: {e}")
# # #             return False
# # #         except Exception as e:
# # #             logger.error(f"Unexpected error sending event: {e}")
# # #             return False
    
# # #     def close(self):
# # #         """Close Kafka producer connection."""
# # #         if self.producer:
# # #             self.producer.flush()
# # #             self.producer.close()
# # #             logger.info("Kafka producer closed")


# # # class SpotifySimulator:
# # #     """Main simulator orchestrating event generation and publishing."""
    
# # #     def __init__(self):
# # #         self.config = Config()
# # #         self.generator = SpotifyEventGenerator(self.config.user_count)
# # #         self.producer = KafkaEventProducer(self.config)
# # #         self.events_sent = 0
# # #         self.events_failed = 0
    
# # #     def print_startup_info(self):
# # #         """Display startup information."""
# # #         logger.info("=" * 60)
# # #         logger.info("🎧 Spotify Event Simulator Starting")
# # #         logger.info("=" * 60)
# # #         logger.info(f"Kafka Brokers: {self.config.kafka_bootstrap_servers}")
# # #         logger.info(f"Kafka Topic: {self.config.kafka_topic}")
# # #         logger.info(f"User Count: {self.config.user_count}")
# # #         logger.info(f"Event Interval: {self.config.event_interval_seconds}s")
# # #         logger.info("-" * 60)
# # #         logger.info("Available Songs:")
# # #         for song_info in self.generator.get_songs_summary():
# # #             logger.info(f"  • {song_info['song']} — {song_info['artist']} ({song_info['song_id'][:8]}...)")
# # #         logger.info("=" * 60)
    
# # #     def print_statistics(self):
# # #         """Print current statistics."""
# # #         total = self.events_sent + self.events_failed
# # #         success_rate = (self.events_sent / total * 100) if total > 0 else 0
# # #         logger.info(
# # #             f"Stats - Sent: {self.events_sent}, Failed: {self.events_failed}, "
# # #             f"Success Rate: {success_rate:.2f}%"
# # #         )
    
# # #     def run(self):
# # #         """Main execution loop."""
# # #         self.print_startup_info()
        
# # #         try:
# # #             while True:
# # #                 event = self.generator.generate_event()
                
# # #                 if self.producer.send_event(event):
# # #                     self.events_sent += 1
# # #                     logger.info(
# # #                         f"✓ [{self.events_sent}] {event.event_type.upper()} - "
# # #                         f"{event.song_name} by {event.artist_name} "
# # #                         f"(user: {event.user_id[:8]}..., device: {event.device_type})"
# # #                     )
# # #                 else:
# # #                     self.events_failed += 1
# # #                     logger.warning(f"✗ Failed to send event {event.event_id}")
                
# # #                 # Print statistics every 100 events
# # #                 if (self.events_sent + self.events_failed) % 100 == 0:
# # #                     self.print_statistics()
                
# # #                 time.sleep(self.config.event_interval_seconds)
                
# # #         except KeyboardInterrupt:
# # #             logger.info("\n⏸️  Simulation interrupted by user")
# # #         except Exception as e:
# # #             logger.error(f"Unexpected error in simulation loop: {e}", exc_info=True)
# # #         finally:
# # #             self.cleanup()
    
# # #     def cleanup(self):
# # #         """Cleanup resources."""
# # #         self.print_statistics()
# # #         self.producer.close()
# # #         logger.info("👋 Simulator shutdown complete")


# # # def main():
# # #     """Application entry point."""
# # #     try:
# # #         simulator = SpotifySimulator()
# # #         simulator.run()
# # #     except ValueError as e:
# # #         logger.error(f"Configuration error: {e}")
# # #         sys.exit(1)
# # #     except Exception as e:
# # #         logger.error(f"Fatal error: {e}", exc_info=True)
# # #         sys.exit(1)


# # # if __name__ == "__main__":
# # #     main()