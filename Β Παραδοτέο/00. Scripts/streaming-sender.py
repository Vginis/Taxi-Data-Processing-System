import asyncio
import csv
import json
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient

EVENT_HUB_CONNECTION_STR = "Endpoint=sb://test-eh123.servicebus.windows.net/;SharedAccessKeyName=testPolicy;SharedAccessKey=J9BKlBKgxz8z6Bcc6iKWPwdYVT1Z5Qkly+AEhLM3AZ0=;EntityPath=eh-test"
EVENT_HUB_NAME = "eh-test"

async def run():
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
    )
    async with producer:
        # Create a batch.
        event_data_batch = await producer.create_batch()

        with open('streaming.csv') as file_obj: 
            reader_obj = csv.reader(file_obj) 
            
            for row in reader_obj: 
                formatted_row = json.dumps(row)
                event_data_batch.add(EventData(formatted_row))
                print(EventData(formatted_row))
                
        
        # Send the batch of events to the event hub.
        await producer.send_batch(event_data_batch)

asyncio.run(run())