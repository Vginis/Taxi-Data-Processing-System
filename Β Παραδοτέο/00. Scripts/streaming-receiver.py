import azure.functions as func
import logging
import json

app = func.FunctionApp()

quadrants = {"Q1": 0, "Q2": 0, "Q3": 0, "Q4": 0}

@app.event_hub_message_trigger(arg_name="azeventhub", event_hub_name="eh-test",
                               connection="EventHubString")

def eventhub_trigger(azeventhub: func.EventHubEvent):
    mylist = []
    mylist.append(azeventhub.get_body().decode('utf-8'))
    
    for row in mylist:
        parsed_row = json.loads(row)

        pickup_lat = float(parsed_row[4])
        pickup_lon = float(parsed_row[3])
        quadrant = get_quadrant(pickup_lat, pickup_lon)
        quadrants[quadrant] += 1   
    logging.info('Python EventHub trigger processed an event: %s %s %s %s',
                    quadrants["Q1"],quadrants["Q2"],quadrants["Q3"],quadrants["Q4"])


def get_quadrant(latitude, longitude):
    center_lat=40.735923
    center_lon=-73.990294
    if latitude >= center_lat:
        if longitude >= center_lon:
            return "Q1"
        else:
            return "Q2"
    else:
        if longitude >= center_lon:
            return "Q3"
        else:
            return "Q4"