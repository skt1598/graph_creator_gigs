from collections import defaultdict 
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import json
import sys
import os
load_dotenv()
sys.path.append(os.path.abspath(os.getenv("SYSTEM_PATH")))
from lib import rabbit_mq, graph


try:
    connection = rabbit_mq.create_connection()
    channel = connection.channel()

    channel.queue_declare(queue='job_nodes_rel')
    channel.queue_declare(queue='hardskill_ml')

    count = 0

    def callback(ch, method, properties, body):
        print('\n\n\nprocessing...')
        global count
        count += 1
        print("count::", count)
        # print(" [x] Received %r" % body)
        job = json.loads(body)
        spacy_tags = job['tags']

        # tags manipulation from list of tuple to dict of list
        print('before tags manipulation::', job)
        tags = defaultdict(list)
        for i, j in job['tags']: 
            tags[i].append(j)

        # Append crawled skills to tags as hardskill
        if 'skills' in job:
            for skill in job['skills']:
                if skill not in tags['HARDSKILL']:
                    tags['HARDSKILL'].append(skill)
        tags = str(dict(tags))
        job['tags'] = tags
        print('after tags manipulation::',job)

        job_res = graph.job_node(job)
        # print('\njob_res::', job_res)
        job_id = job_res[0]['j'].id
        print("job id::", job_id)
        job['job_id'] = job_id

        # create tag node and relation with job
        for tag in spacy_tags:
            print('tag::', tag)
            if tag[0] != 'HARDSKILL':
                graph.tag_node_rel(tag, job_id)

        channel.basic_publish(exchange='', routing_key='hardskill_ml', body=json.dumps(job))

    channel.basic_consume(
        queue='job_nodes_rel', on_message_callback=callback, auto_ack=True)
    # channel.basic_consume(callback, 'remotive_html_parse',  no_ack=False)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
except Exception as e:
    error = {
        "status": "Error occured while creating job nodes and relation !!",
        "errorMsg": e
    }
    print("Error: ",error)