import uuid
import redis
import json
import time
from datetime import datetime
import multiprocessing
import logging
import os


class RedisStreamLogger:
    def __init__(self, redis_client:redis.Redis, stream_key='logs'):
        self.redis_client = redis_client
        self.stream_key = stream_key
        self.added_packets = {}

    async def log(self, level, message, **additional_fields):
        """Log a message to Redis stream"""
        data = {
            'timestamp': datetime.now().isoformat(),
            'level': level,
            'message': message,
            **additional_fields
        }
        await self.redis_client.xadd(self.stream_key, data)

    async def info(self, message):
        await self.log('INFO', message, **self.added_packets )
        logging.info(message)

    async def error(self, message):
        await self.log('ERROR', message, **self.added_packets )
        logging.error(message)
        
    # def exception(self, message, ):
    #     self.log('EXCEPTION', message, self.added_packets )

    async def warning(self, message):
        await self.log('WARNING', message, **self.added_packets )
        logging.warning(message)

    async def debug(self, message):
        await self.log('DEBUG', message, **self.added_packets )
        logging.debug(message)

    async def exception(self, message):
        await self.log('ERROR', message, **self.added_packets )
        logging.exception(message)

class RedisStreamReader(multiprocessing.Process):
    def __init__(self, redis_client, stream_key='logs', consumer_group='log_consumer_group', level=logging.INFO, log_file=f'logs/{datetime.now().replace(microsecond=0)}.log', check_interval=1.0, daemon=True):
        super().__init__()
        self.daemon = daemon
        self.redis_client = redis_client
        self.stream_key = stream_key
        self.consumer_group = consumer_group
        self.consumer_name = f"log_consumer_{uuid.uuid4().hex[:6]}"
        self.log_file = log_file
        self.check_interval = check_interval
        self.setup_logging(level=level)
        self.ensure_consumer_group()

    def ensure_consumer_group(self):
        """Ensure the consumer group exists."""
        try:
            self.redis_client.xgroup_create(
                self.stream_key, self.consumer_group, mkstream=True)
        except redis.exceptions.ResponseError as e:
            # Ignore error if the group already exists
            if "BUSYGROUP" not in str(e):
                raise

    def setup_logging(self, level):
        """Configure logging to file and return the logger."""
        os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
        if not os.path.exists(self.log_file):
            with open(self.log_file, 'w'):
                pass
        logger = logging.getLogger()
        logger.setLevel(level)
        handler = logging.FileHandler(self.log_file)
        formatter = logging.Formatter('%(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def process_message(self, message_id, message):
        """Process a single message from the stream."""
        try:
            timestamp = message[b'timestamp'].decode()
            level = message[b'level'].decode()
            msg = f"{timestamp} - {level} - {message[b'message'].decode()}"

            if level == 'INFO':
                logging.info(msg)
            elif level == 'ERROR':
                logging.error(msg)
            # elif level == "EXCEPTION":
            #     logging.exception(msg)
            elif level == 'WARNING':
                logging.warning(msg)
            elif level == 'DEBUG':
                logging.debug(msg)

            # Acknowledge the message
            self.redis_client.xack(
                self.stream_key, self.consumer_group, message_id)
        except Exception as e:
            logging.error(f"Error processing message: {e}")

    def run(self):
        """Main loop to read from Redis stream."""
        while True:
            try:
                # Read new messages from the stream using XREADGROUP
                messages = self.redis_client.xreadgroup(
                    groupname=self.consumer_group,
                    consumername=self.consumer_name,
                    streams={self.stream_key: '>'},
                    count=10,
                    block=int(self.check_interval * 1000)
                )

                if messages:
                    for stream, message_list in messages:
                        for message_id, message in message_list:
                            self.process_message(message_id, message)

            except Exception as e:
                logging.error(f"Stream reader error: {e}")
                time.sleep(self.check_interval)
