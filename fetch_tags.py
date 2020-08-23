from datetime import datetime, timedelta
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from bson import ObjectId
import cloudscraper
import itertools
import json
import re
import sys
import os
load_dotenv()
sys.path.append(os.path.abspath(os.getenv("SYSTEM_PATH")))
from lib import rabbit_mq, mongo, request


def fetch_tags():
    try:
        # A MongoDB instance for Python
        client = mongo.create_connection()
        db = mongo.set_db(client)
        coll = mongo.set_collection(db, os.getenv('MONGO_COLLECTION'))
        # print(str(date))
        for source in json.loads(os.getenv('GIGS_SOURCE')):
            print('\n\n\nsource::', source)
            jobs = coll.find({'source': source}, no_cursor_timeout=True).sort('date', -1).limit(int(os.getenv("JOB_LIMIT_FOR_TAGGING")))

            # RabbitMq connection
            connection = rabbit_mq.create_connection()
            channel = connection.channel()

            scraper = cloudscraper.create_scraper() 
                
            count = 0
            # model_name = "indeed_may20"
            model_name = "hubstaff"

            print("\nlooping now...")

            tags_obj = {}

            for job in jobs:
            # for elem in jobs:
                try:

                    # tags_obj[elem] = []
                    # job = {}
                    # job['job_link'] = elem

                    count += 1
                    print("\n\n\n\nIteration::", count)
                    print("Job Url::", job['job_link'])
                    tags = []
                    # print("here ")
                    # if count > 10:
                        # break   
                    # if count < 63:
                    #     continue
                    if "description" in job and job['description'] != '':
                        print("Description exists::")
                        print("sending to tagger")
                        payload =  {}
                        payload["text"] = job['title']+' '+job['description']
                        payload["text"] = payload["text"].replace('\n', ' ').replace('. ', ' ').replace(', ', ' ')
                        payload["text"] = re.sub(r'[^A-Za-z0-9- ]+', ' ', payload["text"]).strip()
                        print(payload["text"])
                        payload['model'] = model_name
                        # final_pl = json.dumps(payload)
                        # final_pl = payload
                        tags = request.get_tags(json.dumps(payload))
                        if tags == [None]:
                            tags = []
                    else:
                        print("else ji")
                        print("Description not exists::")
                        print("Job Url::", job['job_link'])
                        url = job['job_link']
                        try:  
                            res = scraper.get(url, timeout=10)
                        except Exception:
                            print("scraper exception !!\nPassing!!")
                            pass
                        print("scraper res status::", res.status_code)
                        if res.status_code == 200:
                            html_text = res.content
                            soup = BeautifulSoup(html_text, 'html.parser')
                            text = soup.find_all(text=True)
                            output = ''
                            blacklist = ['[document]',
                                        'noscript',
                                        'header',
                                        'html',
                                        'meta',
                                        'head', 
                                        'input',
                                        'script',
                                        'style'
                                        # there may be more elements you don'elem want, such as "style", etc.
                                        ]
                            for elem in text:
                                if elem.parent.name not in blacklist:
                                    re.sub(r'\n\s*\n', r'',elem.strip(), flags=re.M)
                                    if len(elem) > 20:
                                        output += elem     
                            # since description has not been fetched during crawling we are using the scraped content
                            job.update({"description": output})
                            description_chunks = [output[i:i+3000] for i in range(0, len(output), 3000)]
                            for chunks in description_chunks:
                                payload =  {}
                                payload["text"] = chunks
                                payload['model'] = model_name
                                resp = get_tags(json.dumps(payload))
                                tags = tags+resp
                            tags.sort()
                            tags = list(tags for tags,_ in itertools.groupby(tags))
                        else:
                            continue
                    # tags_obj[elem] = tags 
                    job.update({"tags": tags})
                    print("\nfinal tags::", tags)
                    print("***done****")
                    job["mongo_id"] = str(job["_id"])
                    del job["_id"]
                    del job["description"]
                    job["date"] = (job["date"] - datetime(1970,1,1)).total_seconds()

                    print("job", job)
                    print("Sending data in queue")
                    channel.queue_declare(queue='job_nodes_rel')
                    channel.basic_publish(exchange='', routing_key='job_nodes_rel', body=json.dumps(job))

                    # print("job::", job)
                    print("-----NEXT-----")
                except Exception as err:
                    print("\nException occured !!", err,"\nContinuing Loop!!")
                    continue
                # break
            print("\n Total count::", count)
            print("\n\n\ntags_obj", tags_obj)
            channel.close()
    except Exception as e:
        # exc_type, exc_obj, exc_tb = sys.exc_info()
        # fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        # print(exc_type, exc_obj, exc_tb)
        error = {
            "status": "Fetch Tags........... Error occured while fetching tags !!",
            # "requestUrl": url,
            "errorMsg": e
        }
        print("Error: ",error)

# fetch_tags()

schedule.every().day.at("08:00").do(fetch_tags)
# schedule.every(10).seconds.do(fetch_tags) 

while True: 
    # Checks whether a scheduled task  
    # is pending to run or not 
    schedule.run_pending() 
    time.sleep(1) 