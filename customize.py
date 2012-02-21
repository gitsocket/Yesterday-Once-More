# -- encoding=utf-8 --

import json
import re
import sys
import datetime

import MySQLdb

#import tracking

def remove_html_tags(data):
    p = re.compile(r'<.*?>')
    return p.sub('', data)


def remove_extra_spaces(data):
    p = re.compile(r'\s+')
    return p.sub(' ', data)


def get_avatar_url(avatar_path, zhihu_cursor):
    hash = avatar_path or '666b0abfc'
    if len(hash) != 9 or '#' in hash:
        zhihu_cursor.execute('SELECT hash FROM upload_mapper WHERE avatar_path = %s', (hash,))
        result = zhihu_cursor.fetchone()
        hash = '666b0abfc' if result is None else result[0]
    return 'http://p1.zhimg.com/' + hash[:2] + '/' + hash[2:4] + '/' + hash + '_m.jpg'

def load_topic_interest(user_id):
    prefix = str(user_id)[:3]
    path = '/data/data/relation/topic_attention/{0}/{1}.csv'.format(prefix, user_id)
    try:
        return open(path).readlines()
    except IOError:
        # Missing interest data file
        return []

def load_member_interest(user_id):
    prefix = str(user_id)[:3]
    path = '/data/data/relation/member_love/{0}/{1}.csv'.format(prefix, user_id)
    try:
        return open(path).readlines()
    except IOError:
        # Missing interest data file
        return []


def customize(member_id, gen_tracking_url, hot_list, zhihu_cursor, zhihu_stats_cursor):
    topic_list = {}
    for result in load_topic_interest(member_id):
        topic_id = int(result.split()[3])
        topic_score = float(result.split()[5])
        topic_list[topic_id] = topic_score
    friend_list = {}
    for result in load_member_interest(member_id):
        member_id = int(result.split()[3])
        member_score = float(result.split()[5])
        friend_list[member_id] = member_score

    visited_answer = []
    result_list = {}
    for i in range(len(hot_list)):
        item = hot_list[i]
        question_topic_list = item['topic_list']
        question_id = item['question']['id']
        answer_id = item['answer']['id']
        new_vote = item['new_vote']
        author_id = item['member']['id']
        result_list[i] = 0.0
        
        #去除已读回答
        if item['answer']['url'] in visited_answer: continue
        sql = "SELECT count(*) FROM visit_member_question WHERE member_id = %s AND question_id = %s"
        zhihu_stats_cursor.execute(sql, (member_id, question_id))
        visit_count = zhihu_stats_cursor.fetchone()[0]
        if visit_count != 0: continue

        sql = "SELECT count(*) FROM visit_member_answer WHERE member_id = %s AND answer_id = %s"
        zhihu_stats_cursor.execute(sql, (member_id, answer_id))
        visit_count = zhihu_stats_cursor.fetchone()[0]
        if visit_count != 0: continue

        # 话题兴趣权重
        visited_answer.append(item['answer']['url'])
        for topic in question_topic_list:
            score = 0.0
            if topic_list.has_key(topic):
                score = topic_list[topic]
                result_list[i] += int(new_vote)*score
                topic_list[topic] = topic_list[topic] * 0.8
        if author_id in friend_list:
            result_list[i] += int(new_vote)*friend_list[author_id]/3
            friend_list[author_id] = friend_list[author_id] * 0.8

    result_list = sorted(result_list.items(),  key=lambda x: x[1],  reverse=True)
    
    content = []
    for i in range(len(result_list)):
        id = result_list[i][0]
        item = hot_list[id]
        question_url = gen_tracking_url(item['question']['url'])
        question_name = item['question']['title']
        vote = item['answer']['vote']
        author_name = item['member']['name']
        author_link = gen_tracking_url(item['member']['url'])
        author_bio = item['member']['bio']
        author_avatar = gen_tracking_url(get_avatar_url(item['member']['avatar'], zhihu_cursor))
        if author_bio is not None and len(author_bio)>0:
            author_bio = u'，'+author_bio
            if len(author_bio)>30:
                author_bio = (author_bio[:20])+u' … ' 

        answer_url = gen_tracking_url(item['answer']['url'])
        answer_preview = item['answer']['content']
        answer_preview = remove_html_tags(answer_preview)
        answer_preview = remove_extra_spaces(answer_preview)

        if len(answer_preview) > 200:
            answer_preview = answer_preview[:200] + u' … '      

        content.append({'question_url': question_url,
                        'question_name': question_name,
                        'vote': vote,
                        'author_name': author_name,
                        'author_link': author_link,
                        'author_avatar': author_avatar,
                        'author_bio': author_bio,
                        'answer_url': answer_url,
                        'answer_preview': answer_preview})

    ctx = {}
    ctx['recommendations'] = content

    return ctx



class Customizer(object):

    def __init__(self):
        connect = MySQLdb.connect(host="10.0.0.113", user="zhihu", passwd="buyaoyongroot", db="zhihu",  charset="UTF8")
        connect_visit = MySQLdb.connect(host="10.0.0.113", user="zhihu", passwd="buyaoyongroot", db="zhihu_stats",  charset="UTF8")
        self.cursor = connect.cursor()
        self.cursor_visit = connect_visit.cursor()
        self.hot_list = json.load(open('/home/zhihu/Yesterday-Once-More/hot.json'))


    def __call__(self, user_id):


        def gen_tracking_url(url):
            return url

        ctx = customize(user_id, gen_tracking_url, self.hot_list, self.cursor, self.cursor_visit)

        ctx['date']            = (datetime.date.today()).strftime("%Y %m %d")

        return ctx

