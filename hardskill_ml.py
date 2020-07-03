from dotenv import load_dotenv
import json
import sys
import re
import os
load_dotenv()
sys.path.append(os.path.abspath(os.getenv("SYSTEM_PATH")))
from lib import rabbit_mq, graph, request

try:
    connection = rabbit_mq.create_connection()
    channel = connection.channel()

    channel.queue_declare(queue='hardskill_ml')
    channel.queue_declare(queue='hardskill_node_rel')

    count = 0

    def callback(ch, method, properties, body):
        print('\n\nprocessing...')
        global count
        count += 1
        print("count::", count)
        # print(" [x] Received %r" % body)
        job = json.loads(body)
        print('job::', job)
        tags = eval(job['tags'])
        print(tags)
        if 'HARDSKILL' in tags.keys():
            for elem in tags['HARDSKILL']:
                slug = '_'.join(elem.split()).lower()
                print('slug::', slug)
                obj = {
                    'job_id': job['job_id'],
                    "skill": slug,
                    'same_skills': []
                }
                skill_exists = graph.hardskill_exists(slug)
                print('skill_exists::', skill_exists)
                if not skill_exists:
                    #similar skils
                    similar_skills = request.similar_skills({"user_skill":slug})
                    if similar_skills and len(similar_skills.keys()) > 0:
                        print("similar_skills::",similar_skills.keys())
                        obj['similar_skills'] = list(similar_skills.keys())

                    # same skills
                    same_skills = request.same_skills({"user_skill":slug})
                    if same_skills:
                        print("same_skills::",same_skills)
                        obj['same_skills'] = same_skills

                #subdomain
                sub_domain = request.sub_domain({"user_skill":slug})
                if sub_domain:
                    print("sub_domain::",sub_domain.keys())
                    print("sub_domain::",sub_domain)
                    obj['sub_domain'] = list(sub_domain.keys())

                print('\n\n\nobj::', obj)
                channel.basic_publish(exchange='', routing_key='hardskill_node_rel', body=json.dumps(obj))


    channel.basic_consume(
        queue='hardskill_ml', on_message_callback=callback, auto_ack=True)
    # channel.basic_consume(callback, 'remotive_html_parse',  no_ack=False)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
except Exception as e:
    error = {
        "status": "Error occured during hardskill ml processing !!",
        "errorMsg": e
    }
    print("Error: ",error)