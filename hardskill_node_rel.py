from dotenv import load_dotenv
import json
import sys
import re
import os
load_dotenv()
sys.path.append(os.path.abspath(os.getenv("SYSTEM_PATH")))
from lib import rabbit_mq, graph

try:
    connection = rabbit_mq.create_connection()
    channel = connection.channel()

    channel.queue_declare(queue='hardskill_node_rel')

    count = 0

    def callback(ch, method, properties, body):
        print('\n\n\n\n\nprocessing...')
        global count
        count += 1
        print("count::", count)
        # print(" [x] Received %r" % body)
        data = json.loads(body)
        print('data::', data)

        # create skill node
        res = graph.skill_node(data)

        # create similar skill node and relation
        if 'similar_skills' in data.keys() and len(data['similar_skills']) > 0:
            print('\nsimilar_skills::', data['similar_skills'])
            for elem in data['similar_skills']:
                print('\nsimilar_skills elem::', elem)
                result = graph.similar_skills_node_rel({'skill': data['skill'], 'slug': elem, 'job_id': data['job_id']})
                print('similar skill result::', result)

        # create subdomain node and relation
        if 'sub_domain' in data.keys() and len(data['sub_domain']) > 0:
            print('\nsub_domain::', data['sub_domain'])
            for elem in data['sub_domain']:
                print('\nsub_domain elem::', elem)
                result = graph.sub_domain_node_rel({'skill': data['skill'], 'slug': elem, 'job_id': data['job_id']})
                print('sub domain result::', result)

    channel.basic_consume(
        queue='hardskill_node_rel', on_message_callback=callback, auto_ack=False)
    # channel.basic_consume(callback, 'remotive_html_parse',  no_ack=False)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
except Exception as e:
    error = {
        "status": "Error occured during hardskill node and relation creation !!",
        "errorMsg": e
    }
    print("Error: ",e)