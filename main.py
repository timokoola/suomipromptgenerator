import base64
import datetime
import json
import os
import functions_framework
from google.cloud import storage
import random

# examples of noun forms
example_noun_forms = {
    "nimento": ["nominatiivi", "nimentö", "mikä? kuka?", "monikossa -t"],
    "kohdanto": ["akkusatiivi", "kohdanto", "kenet?", ""],
    "omanto": [
        "genetiivi",
        "omanto",
        "minkä? kenen?",
        "-n, -en, -in, -den, -ten, -tten",
    ],
    "olento": ["essiivi", "olento", "minä? millaisena? kenenä?", "-na, -nä"],
    "osanto": [
        "partitiivi",
        "osanto, eronto",
        "mitä? ketä?",
        "-a, -ä, -ta, -tä",
    ],
    "tulento": [
        "translatiivi",
        "tulento",
        "miksi? millaiseksi? keneksi?",
        "-ksi -kse (omistusliitteen yhteydessä)",
    ],
    "sisaolento": ["inessiivi", "sisäolento", "missä? kenessä?", "-ssa, -ssä"],
    "sisaeronto": ["elatiivi", "sisäeronto", "mistä? kenestä?", "-sta, -stä"],
    "sisatulento": [
        "illatiivi",
        "sisätulento",
        "mihin? keneen?",
        "-loppuvokaalin pidentymä + n  -han, hen, hin, hon, hun, hyn, hän, hön  -seen, -siin",
    ],
    "ulkoolento": ["adessiivi", "ulko-olento", "millä? kenellä?", "-lla, -llä"],
    "ulkoeronto": ["ablatiivi", "ulkoeronto", "miltä? keneltä)", "-lta, -ltä"],
    "ulkotulento": ["allatiivi", "ulkotulento", "mille? kenelle?", "-lle"],
    "vajanto": ["abessiivi", "vajanto", "mitä ilman?", "-tta, -ttä"],
    "keinonto": ["instruktiivi", "keinonto", "miten (keinoa, välinettä)", "-n"],
    "seuranto": ["komitatiivi", "seuranto", "minkä kanssa", "-in, -ine-"],
}

gradations = {
    "A": {"k": "kk", "kk": "k"},
    "B": {"p": "pp", "pp": "p"},
    "C": {"t": "tt", "tt": "t"},
    "D": {"k": "-", "-": "k"},
    "E": {"p": "v", "v": "p"},
    "F": {"t": "d", "d": "t"},
    "G": {"nk": "ng", "ng": "nk"},
    "H": {"mp": "mm", "mm": "mp"},
    "I": {"lt": "ll", "ll": "lt"},
    "J": {"nt": "nn", "nn": "nt"},
    "K": {"rt": "rr", "rr": "rt"},
    "L": {"l": "k", "k": "l"},
    "M": {"k": "v"},
    "_": {"_": "_"},
}


def pretty_print_gradation(gradation):
    result = []
    for k, v in gradations[gradation].items():
        result.append(f"{k} -> {v}")
    return ", ".join(result)


# Triggered by a change in a topic
@functions_framework.cloud_event
def generate_prompt(cloud_event):
    # Print out the data from Pub/Sub, to prove that it worked
    print(base64.b64decode(cloud_event.data["message"]["data"]))

    # assert that the environment variable OUTPUT_BUCKET is set
    assert os.environ.get("OUTPUT_BUCKET") is not None

    # get the  output bucket from the environment variable
    output_bucket = os.environ.get("OUTPUT_BUCKET")

    # create a client
    storage_client = storage.Client()
    # get the bucket
    bucket = storage_client.get_bucket(output_bucket)

    # prompts markdown file
    prompt_file = "prompts.md"

    # check that output bucket has the prompts file
    if prompt_file in [blob.name for blob in bucket.list_blobs()]:
        # get the blob
        blob = bucket.get_blob(prompt_file)
        # get the content
        content = blob.download_as_string()
        # decode the content
        content = content.decode("utf-8")
    else:
        content = ""

    # create downloads folder if it doesn't exist
    if not os.path.exists("downloads"):
        os.mkdir("downloads")

    # download the all.csv file which might have been updated
    blob = bucket.get_blob("all.csv")
    blob.download_to_filename("downloads/all.csv")

    # download the kotus_all.json file but only if it doesn't exist
    if not os.path.exists("downloads/kotus_all.json"):
        blob = bucket.get_blob("kotus_all.json")
        blob.download_to_filename("downloads/kotus_all.json")

    # open the all.csv file
    with open("downloads/all.csv", "r") as f:
        # read the lines
        lines = f.readlines()

    # open the kotus_all.json file
    with open("downloads/kotus_all.json", "r") as f:
        # read the lines and parse the json
        kotus_all = json.load(f)

    #  The output is in CSV format:
    # isoformat_timestamp,filename,sijamuoto,number,av,tn,count
    # 2020-01-01T12:00:00,filename,sisatulento,plural,D,10,1123

    # dictionary to hold the counts
    counts = {}

    for line in lines:
        # split the line into fields
        fields = line.split(",")
        # get the filename
        # get the sijamuoto
        sijamuoto = fields[2]
        # get the number
        number = fields[3]
        # get the av
        av = fields[4]
        # get the tn
        tn = fields[5]

        # combine the sijamuoto, number, av and tn to a key with ':' as separator
        key = ":".join([sijamuoto, number, av, tn])

        # get the count
        count = int(fields[6], 10)

        # add the count to the dictionary
        if key in counts:
            counts[key] += count
        else:
            counts[key] = count

    # find the maximum count
    max_count = max(counts.values())

    # filter the counts to only include those that are less than 5% of the maximum
    counts = {k: v for k, v in counts.items() if v < max_count * 0.05}

    # pick up to 10 random keys
    keys = random.sample(list(counts.keys()), min(10, len(counts)))

    ## add a markdown level 2 header with the date
    content += f"## Prompts for {datetime.datetime.now().isoformat()}\n"

    for key in keys:
        # split the key
        sijamuoto, number, av, tn = key.split(":")

        # from kotus_all.json get the words av and tn
        words = [k for k in kotus_all if k["av"] == av and k["tn"] == int(tn)]

        pick = random.choice(words)

        example = " ".join(example_noun_forms[sijamuoto])
        prompt_line = f"- {pick['word']} Astevaihtelu: {pretty_print_gradation(av)} ({number} {example})\n"

        # add the pick to the content
        content += prompt_line

    print(content)

    # append the content to the prompts file
    with open(prompt_file, "a") as f:
        f.write(content)

    # upload the prompts file to the output bucket
    blob = bucket.blob(prompt_file)
    blob.upload_from_filename(prompt_file)
