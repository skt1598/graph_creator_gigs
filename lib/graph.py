from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

load_dotenv()

relation_type = {
    'ORG': 'BELONGS_TO',
    'ROLE': 'HAS',
    'COUNTRY': 'LOCATED_IN',
    'SOFTSKILL': 'REQUIRES',
    'HARDSKILL': 'REQUIRES',
    'DOMAIN': 'BELONGS_TO',
    'SALARY': 'HAS',
    'DURATION': 'HAS',
    'NATIONALITY': 'REQUIRES',
    'URL': 'HAS',
    'EMAIL': 'HAS',
    'PERSONNAME': 'HAS',
    'COMPANYNAME': 'HAS',
    'DATE': 'HAS',
    'PRODUCT': 'HAS',
    'TECHNOLOGY': 'HAS',
    'EDUCATIONQUALIFICATION': 'HAS',
    'CITY': 'LOCATED_IN',
    'TYPEOFWORK': 'HAS',
    'ACTIVITY': 'REQUIRES',
    'BENEFIT': 'REQUIRES',
    'REQUIREMENT': 'REQUIRES',
    'SOCIALCONTEXT': 'HAS',
    'LOCATION': 'LOCATED_IN',
    'EXPERIENCE': 'REQUIRES',
    'JOBROLE': 'HAS',
    'LANGUAGE': 'REQUIRES'
}

# def create_conn(GRAPH_URI=os.getenv("GRAPH_URI"), user=os.getenv("GRAPH_USER"), password=os.getenv("GRAPH_PASS")):
#     uri = GRAPH_URI
#     driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=False)
#     graph = driver.session()
#     return graph

uri = os.getenv("GRAPH_URI")
driver = GraphDatabase.driver(uri, auth=("neo4j", "admin@123"), encrypted=False)

def job_node(job):
    # graph = create_conn()
    graph = driver.session()
    create_job = "MERGE (j:JOB{job_link:$job_link, date: $date, tags: $tags, title: $title, crawled_domain: $crawled_domain, source: $source}) RETURN j"
    res = graph.run(create_job, job_link = job['job_link'], date = job['date'], tags = job['tags'], title = job['title'], crawled_domain = job['domain'], source = job['source']).data()
    graph.close()
    return res

def tag_node_rel(tag,job_id):
    graph = driver.session()
    obj = {
        'slug':  '_'.join(tag[1].split()).lower(),
        "display_val": tag[1].title()
    }
    if tag[0] in relation_type:
        rel = relation_type[tag[0]]
    else:
        rel = 'REQUIRES'
    create_node_rel = "MATCH (j:JOB) WHERE id(j) = $job_id MERGE (n:"+tag[0]+ "{slug: $slug, display_val: $display_val}) MERGE (j) - [r:"+rel+"] -> (n) RETURN j,r,n"
    res = graph.run(create_node_rel, slug = obj['slug'], display_val = obj['display_val'], job_id = job_id, rel = rel)
    graph.close()
    return res

def hardskill_exists(skill_slug):
    graph = driver.session()
    query = "MATCH (n: HARDSKILL) WHERE n.slug = $skill_slug RETURN n"
    res = graph.run(query, skill_slug = skill_slug).data()
    if len(res) > 0:
        return True
    return False

def skill_node(body):
    graph = driver.session()
    obj = {
        "slug": body['skill'],
        "display_val": ' '.join(body['skill'].split('_')).title()
    }
    create_skill = "MATCH (j:JOB) WHERE id(j) = $job_id MERGE (h: HARDSKILL{slug: $slug}) SET h = {slug: $slug, display_val: $display_val, same_skills: $same_skills}  MERGE (j) - [r:REQUIRES] -> (h) RETURN j,r,h"
    res = graph.run(create_skill, job_id = body['job_id'], slug = obj['slug'], display_val = obj['display_val'], same_skills = body['same_skills']).data()
    return res

def similar_skills_node_rel(body):
    graph = driver.session()
    obj = {
        "slug": body['slug'],
        "display_val": ' '.join(body['slug'].split('_')).title()
    }
    create_skill = "MATCH (j:JOB) WHERE id(j) = $job_id MATCH (n: HARDSKILL) WHERE n.slug = $skill_slug MERGE (h: HARDSKILL{slug: $slug}) SET h.display_val = $display_val MERGE (n) - [s:SIMILAR_TO] -> (h) MERGE (j) - [r:INDIRECT_REQUIRES] -> (h) RETURN n,s,h,r,j"
    res = graph.run(create_skill, job_id = body['job_id'], skill_slug = body['skill'], slug = obj['slug'], display_val = obj['display_val']).data()
    return res

def sub_domain_node_rel(body):
    graph = driver.session()
    obj = {
        "slug": body['slug'],
        "display_val": ' '.join(body['slug'].split('_')).title()
    }
    create_sub_domin = "MATCH (j:JOB) WHERE id(j) = $job_id MATCH (n: HARDSKILL) WHERE n.slug = $skill_slug MERGE (d: DOMAIN{slug: $slug, display_val: $display_val}) MERGE (n) - [r:BELONGS_TO] -> (d) MERGE (j) - [z:BELONGS_TO] -> (d) RETURN n,r,d,z,j"
    res = graph.run(create_sub_domin, job_id = body['job_id'], skill_slug = body['skill'], slug = obj['slug'], display_val = obj['display_val']).data()
    return res

def domain_personality_rel(body):
    graph = driver.session()
    obj = {
        "domain_adaptiv_id": body['domain'].lower(),
        "domain_display_val": ' '.join(body['domain'].split('_')).title(),
        "personality_adaptiv_id": body['personality'].lower(),
        "personality_type": ' '.join(body['personality'].split('_')).title()
    }
    create_domain_personality_rel = "MERGE (d: DOMAIN{adaptiv_id: $domain_adaptiv_id}) set d = {adaptiv_id: $domain_adaptiv_id, display_val: $domain_display_val} MERGE (p: PERSONALITY{adaptiv_id: $personality_adaptiv_id, type: $personality_type}) MERGE (p) - [r:REQUIRES] -> (d) RETURN p, r, d"
    res = graph.run(create_domain_personality_rel, domain_adaptiv_id = obj['domain_adaptiv_id'], domain_display_val = obj['domain_display_val'], personality_adaptiv_id = obj['personality_adaptiv_id'], personality_type = obj['personality_type']).data()
    return res

def course_node(body):
    graph = driver.session()
    obj = {
        "title": body['title'],
        "courseUrl": body['courseUrl']
    }
    create_course = "MERGE (c: COURSE{course_url: $courseUrl}) SET c = {course_url: $courseUrl, title: $title, type: $type} RETURN c"
    res = graph.run(create_course, title = body['title'], type = body['type'], courseUrl = body['courseUrl']).data()
    graph.close()
    return res

def course_domain_rel(body):
    graph = driver.session()
    obj = {
        "domain_adaptiv_id": body['domain'].lower(),
        "domain_display_val": ' '.join(body['domain'].split('_')).title()
    }
    create_domain_course_rel = "MATCH (c:COURSE) WHERE id(c) = $course_id MERGE (d: DOMAIN{adaptiv_id: $domain_adaptiv_id, display_val: $domain_display_val}) MERGE (c) - [r:REQUIRES] -> (d) RETURN c, r, d"
    res = graph.run(create_domain_course_rel, course_id = body['course_id'], domain_adaptiv_id = obj['domain_adaptiv_id'], domain_display_val = obj['domain_display_val']).data()
    return res

def course_domain_level_rel(body):
    graph = driver.session()
    obj = {
        "common_name": body['level'],
        "proper_name": '_'.join(body['level'].split()).lower(),
        "adaptiv_id": '_'.join(body['level'].split()).lower()
    }
    create_course_level_rel = "MATCH (c:COURSE) WHERE id(c) = $course_id MATCH (d:DOMAIN) WHERE id(d) = $domain_id MERGE (l: LEVEL{adaptiv_id: $adaptiv_id, common_name: $common_name, proper_name: $proper_name, description: ''}) MERGE (c) - [h:HAS] -> (l) MERGE (l) - [r:requires] -> (d) RETURN c,h,l,r,d"
    res = graph.run(create_course_level_rel, course_id = body['course_id'], domain_id = body['domain_id'], common_name = body['level'], proper_name = obj['proper_name'], adaptiv_id = obj['adaptiv_id']).data()
    return res
