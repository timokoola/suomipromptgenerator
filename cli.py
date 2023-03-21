import base64
import json
from main import generate_prompt


# cloud event data object
class CloudEventData:
    def __init__(self, data):
        self.data = data


# just a tester of the cloud function

if __name__ == "__main__":
    # base64 encoded data
    innerData = base64.b64encode(
        json.dumps({"message": {"data": "test"}}).encode("utf-8")
    )

    # data is a dictionary
    data = {}
    data["message"] = {}
    data["message"]["data"] = innerData

    cloudEventData = CloudEventData(data)

    generate_prompt(cloudEventData)
