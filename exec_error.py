import asyncio
import json
import logging
import aioredis
from motor.motor_asyncio import AsyncIOMotorClient
from typing import Dict, Any, List, Optional
from configparser import ConfigParser
import os
import sys
from stream_logger import RedisStreamLogger
import datetime
import time
from secrets import token_hex
import random
import uvloop
from urllib.parse import quote_plus

# Use uvloop for better performance
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

class ExecErrorProcessor:
    """
    Processes pending orders using a Redis queue approach.
    1. Reads orders from input stream and pushes to Redis queue
    2. Pops orders from queue, processes them, and either completes or re-queues
    3. Simple, no stream complexity, easier to monitor and maintain
    """

    def __init__(
        self, 
        config: ConfigParser,
        error_stream,
        logging_stream,
        processing_concurrency,
    ):
        
        self.config = config

        self.consumer_group = sys.argv[1]
        self.consumer_name = sys.argv[2]
        
        # Set up logging
        logdir = f'{self.consumer_group}_Logs/{str(datetime.date.today())}/'
        if not os.path.exists(logdir):
            os.makedirs(logdir)
        logfile = f'{logdir}{self.consumer_name}.log'

        logging.basicConfig(
            level=logging.INFO,
            filename=logfile,
            filemode='a',
            force=True,
            format='%(asctime)s - %(levelname)s - %(message)s',
        )

        logging.info(f"Starting Queue-Based Order Processor for {self.consumer_group} - {self.consumer_name}")

        self.error_stream = error_stream
        self.logging_stream = logging_stream
                
        # Concurrency control
        self.processing_concurrency = processing_concurrency
        self.processing_semaphore = asyncio.Semaphore(processing_concurrency)
        
        self.tasks = set()

        # For graceful shutdown
        self.is_running = False

    async def get_redis_conn(self, db:int = 0, key = 'dbParams') -> aioredis.Redis:
        try:
            host = self.config[key]['redisHost']
            port = int(self.config[key]['redisPort'])
            password = self.config[key]['redisPass']
            redis = await aioredis.Redis(
                host=host,
                port=port,
                password=password,
                db=db,
                decode_responses=True
            )
            logging.info(f"Connected to Redis at {host}:{port}, db {db}")
            return redis
        except Exception as e:
            logging.exception(f"Failed to connect to Redis: {str(e)}")
            raise

    async def get_mongo_conn(self):
        try:
            mongo_host = self.config['infraParams']['mongoHost']
            mongo_port = int(self.config['infraParams']['mongoPort'])
            mongo_user = self.config['infraParams']['mongoUser']
            mongo_pass = self.config['infraParams']['mongoPass']

            mongo_user = quote_plus(mongo_user)  # URL encode username
            mongo_pass = quote_plus(mongo_pass)  # URL encode password
            # Construct MongoDB URI
            mongo_uri = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:{mongo_port}/"
            
            # Create MongoDB client and database reference
            mongo_client = AsyncIOMotorClient(mongo_uri)
            logging.info(f"Connected to MongoDB at {mongo_host}:{mongo_port}")
            return mongo_client
        except Exception as e:
            logging.exception(f"Failed to connect to MongoDB: {str(e)}")
            raise
    
    async def initialize(self):
        """Initialize connections and set up consumer group."""
        logging.info("Initializing Queue-Based Order Processor")
                
        # Connect to Redis for various needs
        self.sym_redis = await self.get_redis_conn(db=0)
        self.master_redis = await self.get_redis_conn(db=11)
        self.stream_redis = await self.get_redis_conn(db=12, key='infraParams')
        self.algo_redis = await self.get_redis_conn(db=8, key='infraParams')
        self.ping_redis = await self.get_redis_conn(db=9, key='infraParams')

        self.logger = RedisStreamLogger(self.stream_redis, self.logging_stream)
        self.logger.added_packets.update({'consumer_group': self.consumer_group, 'consumer_name': self.consumer_name})
        
        self.db_mongo_client = await self.get_mongo_conn()
        self.orders_db = self.db_mongo_client['symphonyorder_raw'][f'orders_{str(datetime.date.today())}']
        self.response_db = self.db_mongo_client['final_response'][f'final_response_{str(datetime.date.today())}']
        self.monitor_db = self.db_mongo_client['algo_status'][f'algo_message']
        self.firewalldb = self.db_mongo_client['Client_Strategy_Status']['Client_Strategy_Status']
        # Create consumer group for input stream
        await self.create_consumer_group(self.error_stream)

    async def create_consumer_group(self, stream_name: str):
        try:
            await self.stream_redis.xgroup_create(
                stream_name, 
                self.consumer_group,
                id='0',  # Start from beginning of stream
                mkstream=True  # Create stream if it doesn't exist
            )
            logging.info(f"Created consumer group {self.consumer_group} for stream {stream_name}")
        except aioredis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logging.info(f"Consumer group {self.consumer_group} already exists for stream {stream_name}")
            else:
                raise

    async def close(self):
        """Close all connections."""
        self.is_running = False
        
        # Close Redis connections
        if self.sym_redis:
            await self.sym_redis.close()
        if self.master_redis:
            await self.master_redis.close()
        if self.stream_redis:   
            await self.stream_redis.close()
        if self.ping_redis:
            await self.ping_redis.close()
        if self.db_mongo_client:
            self.db_mongo_client.close()
        logging.info("Closed all connections")

    async def get_current_ltp(self, sym_id: int) -> float:
        """Get the current Last Traded Price for a symbol."""
        try:
            ltp = float(await self.sym_redis.get(str(sym_id)))
            return ltp
        except Exception as e:
            logging.exception(f"Error getting current LTP for {sym_id}: {str(e)}")
            return None

    async def check_order_status(self, app_order_id: str) -> bool:
        try:
            order_status = await self.orders_db.distinct('responseFlag',{'appOrderID': app_order_id})
            if not order_status:
                await self.logger.warning(f"Order {app_order_id} not found in database")
                return False
            return order_status[0]
        except Exception as e:
            await self.logger.exception(f"Error checking order status for {app_order_id}: {str(e)}")
            return False

    def get_unique_id(self) -> str:
        timestamp = time.time()  # Current Unix timestamp in milliseconds
        random_token = token_hex(4)  # Generate a random 16-character hex token
        return f"{timestamp}{random_token}"
    
    async def claim_pending_messages(self):
        """
        Look for any pending messages in the input stream that weren't processed and push them to queue.
        """
        while self.is_running:
            try:
                # Read pending messages for our consumer
                pending = await self.stream_redis.xpending_range(
                    self.error_stream,
                    self.consumer_group,
                    min='-',
                    max='+',
                    count=10
                )
                
                if not pending:
                    # No pending messages, check again later
                    await asyncio.sleep(60)
                    continue
                
                for p in pending:

                    message_id = p['message_id']
                    
                    # Claim the message
                    claimed = await self.stream_redis.xclaim(
                        self.error_stream,
                        self.consumer_group,
                        self.consumer_name,
                        min_idle_time=60 * 1000,  # 1 minute in milliseconds
                        message_ids=[message_id]
                    )
                    
                    if not claimed:
                        continue
                    
                    for c_message_id, fields in claimed:
                        if not c_message_id: 
                            continue
                        
                        error_data:dict = fields
                        
                        task = asyncio.create_task(self.process_error(message_id, error_data))
                        self.tasks.add(task)
                        task.add_done_callback(self.tasks.discard)
                                                

                        await self.logger.info(f"Claimed and requeued error with {c_message_id}")
                
            except Exception as e:
                await self.logger.exception(f"Error claiming pending messages: {str(e)}")
                await asyncio.sleep(10)

    async def process_error(self, message_id: str, error_data: dict):
        """
        Process a single error:
        
        Args:
            message_id: The Redis message ID
            error_data: The error data to process
        """
        async with self.processing_semaphore:
            try:
                await self.logger.info(f"Processing error: {error_data}")
                
                if 'order_data' in error_data:
                    message = error_data['message']
                    order_data = json.loads(error_data['order_data'])
                    await self.logger.info(f'Error from Executor_Client: {message} in order : {order_data}')
                    await self.close_exec_gate(order_data)
                    await self.logger.info(f'Closing execution gate for order: {order_data}')
                    await self.log_error_order(order_data, message)
                    await self.send_alarm(order_data,message)
                    await self.stream_redis.xack(self.error_stream, self.consumer_group, message_id)
                    await self.logger.info(f"Processed error: {message} for order: {order_data}")
                
                elif 'pending_order' in error_data:
                    message = error_data['message'] 
                    pending_data = json.loads(error_data['pending_order'])
                    await self.logger.info(f'Error from Executor_Client: {message} in pending order : {pending_data}')
                    await self.stream_redis.xack(self.error_stream, self.consumer_group, message_id)
                    await self.logger.info(f"Processed error: {message} for pending order: {pending_data}")
                
                elif 'error_type' in error_data:
                    if error_data['error_type'] == 'AlgoSignalValidationError':
                        message = error_data['message']
                        order_data:dict = json.loads(error_data['algo_signal'])
                        if 'algoName' not in order_data:  
                            order_data['algoName'] = 'UnknownAlgo'
                            order_data['clientID'] = 'UnknownClient'
                            await self.log_error_order(order_data, message)
                            await self.stream_redis.xack(self.error_stream, self.consumer_group, message_id)
                            await self.logger.info(f"Processed error: {message} for order: {order_data}")
                            await self.send_alarm(order_data, message='AlgoName not found')
                            return 
                        
                        client_action_map:dict = await self.stream_redis.hgetall(f'client_action_map: {order_data["algoName"]}')
                        await self.logger.info(f'Client action map: {client_action_map}')
                        await self.logger.info(f'Error from Executor_RMS: {message} in algo signal : {order_data}')
                        # await self.firewalldb.update_many({'algoname': order_data['algoName']}, {'$set':{'Start_Stop':"STOP"}})
                        
                        for client_id, status in client_action_map.items():
                            if status == '1':
                                temp_order_data = order_data.copy()
                                temp_order_data['clientID'] = client_id
                                await self.close_exec_gate(temp_order_data)
                                await self.log_error_order(temp_order_data, message)
                                
                        await self.send_alarm(order_data,message)
                        await self.stream_redis.xack(self.error_stream, self.consumer_group, message_id)
                        await self.logger.info(f"Processed error: {message} for signal: {order_data}")
                    
                    elif error_data['error_type'] == 'ClientOrderValidationError':
                        message = error_data['message']
                        order_data = json.loads(error_data['client_order'])
                        await self.logger.info(f'Error from Executor_RMS: {message} in order : {order_data}')
                        await self.close_exec_gate(order_data)
                        await self.log_error_order(order_data, message)
                        await self.send_alarm(order_data,message)
                        await self.stream_redis.xack(self.error_stream, self.consumer_group, message_id)
                        await self.logger.info(f"Processed error: {message} for order: {order_data}")
                
            except Exception as e: 
                await self.logger.exception(f"Error processing message {message_id}: {str(e)}")
                # Acknowledge the message even if processing fails
                await self.stream_redis.xack(self.error_stream, self.consumer_group, message_id)
                return
            
    async def send_alarm(self, order_data: dict, message: str, alarm:str = 'reject_rms'):
        """
        Send an alarm for the error.
        
        Args:
            order_data: The order data to process
            message: The error message
        """
        try:
            await self.logger.info(f"Sending alarm for error: {message}")
            await self.ping_redis.rpush('alarms-0', alarm)
           
            await self.logger.info(f"Alarm sent for error: {message}")
        except Exception as e:
            await self.logger.exception(f"Error sending alarm: {str(e)}")
            # Acknowledge the message even if processing fails
            await self.stream_redis.xack(self.error_stream, self.consumer_group, order_data['message_id'])
            return
        
    async def get_symbol(self, exchange_instrument_id: str, exchange_segment: str) -> str:
        try:
            symbol = await self.sym_redis.get(f"{exchange_instrument_id}_{exchange_segment}")
            if not symbol:
                await self.logger.warning(f"Symbol not found for {exchange_instrument_id}_{exchange_segment}")
                return 'Not Found'
            return symbol
        except Exception as e:
            await self.logger.exception(f"Error getting symbol: {str(e)}")
            # Acknowledge the message even if processing fails
            return 'Not Found'

    async def log_error_order(self, order_data: dict, message: str):
        """
        Log the error order to the database.
        
        Args:
            order_data: The order data to log
        """
        try:
            symbol = order_data.get('symbol')
            if 'symbol' not in order_data:
                symbol = await self.get_symbol(order_data['exchangeInstrumentID'], order_data['exchangeSegment'])
            await self.logger.info(f"Logging error order: {order_data}")
            final_post = {
                'quantity': order_data['orderQuantity'],
                'algoname': order_data['algoName'],
                'symbol': symbol,
                'exchangeInstrumentID': order_data['exchangeInstrumentID'],
                'exchangeSegment': order_data['exchangeSegment'],
                'buy_sell': order_data['orderSide'],
                'time_stamp': str(datetime.datetime.now().strftime("%H:%M:%S")),
                'productType': order_data['productType'],
                'orderStatus': 'Cancelled',
                'cancelrejectreason': f'M&M RMS: {message}',
                'OrderAverageTradedPrice': 0,
                'clientID': order_data['clientID'],
                'OrderUniqueIdentifier': f'ErrorOrder_{token_hex(5)}',
                'orderSentTime':time.time(),
                'orderType':order_data['orderType']
            }
            await self.response_db.insert_one(final_post)
            await self.monitor_db.insert_one({'executor': f"{order_data['algoName']}_{order_data['clientID']}", 'type': 'error','message': message, 'time_stamp': str(datetime.datetime.now())})
            await self.logger.info(f"Logged error order: {final_post}")
        except Exception as e:
            await self.logger.exception(f"Error logging order: {str(e)}")
            # Acknowledge the message even if processing fails
            await self.stream_redis.xack(self.error_stream, self.consumer_group, order_data['message_id'])
            return
            
    async def close_exec_gate(self, order_data: dict):
        """
        Close the execution gate for a given order.
        
        Args:
            order_data: The order data to process
        """
        try: 
            client = order_data['clientID']
            algo = order_data['algoName']
            await self.algo_redis.set(f'{algo}_{client}',json.dumps({'action':"Stop"}))
            # await self.logger.info(f"Closing execution gate for client {client} and algo {algo}")
            # redis_key = f'client_action_map: {algo}'
            # await self.stream_redis.hset(redis_key, client, 0)
            await self.logger.info(f"Closed execution gate for client {client} and algo {algo}")
        except Exception as e:
            await self.logger.exception(f"Error closing execution gate: {str(e)}")
            # Acknowledge the message even if processing fails
            await self.stream_redis.xack(self.error_stream, self.consumer_group, order_data['message_id'])
            return
        
    async def listen_for_errors(self):
        """
        Main loop to listen for new errors on the Redis stream.
        """
        self.is_running = True
        
        await self.logger.info(f"Starting to listen on stream {self.error_stream}")
        group_info = await self.stream_redis.xinfo_groups(self.error_stream)
        await self.logger.info(f"Consumer group info: {group_info}")

        stream_info = await self.stream_redis.xinfo_stream(self.error_stream)
        await self.logger.info(f"Stream info: {stream_info}")

        while self.is_running:
            try:
                # Read new messages from the stream
                messages = await self.stream_redis.xreadgroup(
                    self.consumer_group,
                    self.consumer_name,
                    {self.error_stream: '>'},  # > means new messages only
                    count=10,  # Process 10 messages at a time
                    block=2000  # Block for 2 seconds if no messages
                )

                if not messages:
                    continue
                # Process each message

                for stream, stream_messages in messages:
                    await self.logger.info(f"Received {len(stream_messages)} messages from stream {stream}")
                    for message_id, fields in stream_messages:

                        self.logger.added_packets.update({'error_id': message_id})

                        await self.logger.info(f"Message fields: {fields}")

                        # Parse JSON if the order data is stored as JSON string
                        if 'data' in fields and isinstance(fields['data'], str):
                            try:
                                error_data = json.loads(fields['data'])
                            except json.JSONDecodeError:
                                await self.logger.error(f"Invalid JSON in message: {fields['data']}")
                                await self.stream_redis.xack(self.error_stream, self.consumer_group, message_id)
                                continue
                        else: error_data = fields

                        # Start processing task
                        task = asyncio.create_task(self.process_error(message_id, error_data))
                        self.tasks.add(task)
                        task.add_done_callback(self.tasks.discard)
                
            except aioredis.ResponseError as e:
                error_msg = str(e).upper()
                if any(keyword in error_msg for keyword in ['NOGROUP', 'NOSTREAM']):
                    await self.logger.error(f"Stream or consumer group missing: {str(e)}")
                    await self.logger.info("Attempting to recreate consumer group...")
                    try:
                        await self.create_consumer_group(self.error_stream)
                        await self.logger.info("Consumer group recreated successfully")
                    except Exception as recreate_error:
                        await self.logger.exception(f"Failed to recreate consumer group: {recreate_error}")
                        # Implement exponential backoff to prevent tight loops
                        await asyncio.sleep(10)
                else:
                    await self.logger.exception(f"Redis response error: {str(e)}")
                    await asyncio.sleep(5)
            
            except asyncio.CancelledError:
                await self.logger.info("Order listener cancelled")
                break
                
            except Exception as e:
                await self.logger.exception(f"Error in order listener: {str(e)}")
                # Sleep briefly to prevent tight loop on persistent errors
                await asyncio.sleep(1)



    async def trim_streams(self):
        """
        Periodically trim Redis streams to prevent them from bloating.
        """
        while self.is_running:
            try:
                # Trim input and output streams to reasonable lengths
                await self.stream_redis.xtrim(self.error_stream, maxlen=10000, approximate=True)
                
                await self.logger.info("Trimmed streams")
            except Exception as e:
                await self.logger.exception(f"Error in trim_streams: {str(e)}")
            
            # Wait before trimming again
            await asyncio.sleep(60)

    async def run(self):
        """
        Run the queue-based order processor.
        """
        await self.initialize()
        self.is_running = True
        
        error_processor = asyncio.create_task(self.listen_for_errors())

        # Start the pending message claimer
        claim_task = asyncio.create_task(self.claim_pending_messages())
        
        # Start stream trimmer
        trimmer_task = asyncio.create_task(self.trim_streams())
        
        # Combine all tasks
        all_tasks = [error_processor, claim_task, trimmer_task]
        
        try:
            # Run until interrupted
            await asyncio.gather(*all_tasks)
        finally:
            await self.close()


async def main():
    """Main entry point for the application."""
    # Load configuration
    config = ConfigParser()
    config.read('config.ini')

    payload = {
        'config': config,
        'error_stream': config['params']['error_stream'],
        'logging_stream': config['params']['logging_stream'],
        'processing_concurrency': int(config['params']['processing_concurrency']),
    }
    
    # Create and run the queue-based order processor
    processor = ExecErrorProcessor(**payload)
    
    try:
        await processor.run()
    except KeyboardInterrupt:
        logging.info("Shutting down")
        await processor.close()


if __name__ == "__main__":
    asyncio.run(main())