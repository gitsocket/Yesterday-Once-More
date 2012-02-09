#!/usr/bin/env python
#coding=utf-8

import sys
import datetime
import time
import json
from operator import itemgetter

import MySQLdb


mysql_host = '10.0.0.113'
mysql_user = 'zhihu'
mysql_password = 'buyaoyongroot'
mysql_db = 'zhihu'
people_rank_file = '/home/zhihu/people_rank.list'
day_interval = 1
editor_value = 10
value_threshold = 20
abandoned_topic_list = [2, 662, 4242, 25838, 1309, 495] #[知乎, 做爱, 性, 性交易, 调查类问题, X是谁]


def get_time_interval(interval = 7):
    '''
    获取目标的时间段的时间
    '''
    today = datetime.date.today()
    end_date = datetime.datetime(year = today.year, month = today.month,
                                 day = today.day)
    begin_date = end_date - datetime.timedelta(days = interval)
    end_time = time.mktime(end_date.timetuple())
    begin_time = time.mktime(begin_date.timetuple())
    return begin_time,end_time

def get_people_rank_dict(file_name = people_rank_file):
    '''
    获取PeopleRank文件列表, 存入词典
    '''
    people_rank_list = open(people_rank_file).readlines()
    people_rank_dict = {}
    for line in people_rank_list:
        sp = line.split('\t')
        people_rank_dict[int(sp[0])] = int(sp[1])
    return people_rank_dict

def get_mysql_cursor(host, user, password, database):
    connect = MySQLdb.connect(host = host, user = user,
                              passwd = password, db = database)
    cursor = connect.cursor()
    return cursor


def main():
    #获取时间
    begin_time,end_time = get_time_interval(day_interval)
    #获取PeopleRank词典
    people_rank_dict = get_people_rank_dict(people_rank_file)
    #获取MySQL的cursor接口
    cursor = get_mysql_cursor(mysql_host, mysql_user, mysql_password, mysql_db)

    #获取获得投票的回答列表
    sql = 'SELECT answer_id,member_id,vote FROM answer_vote WHERE created > %s and created < %s'
    cursor.execute(sql, (begin_time,end_time))
    hot_answers = {}
    while (1):
        result =cursor.fetchone()
        if result == None:
            break
        answer_id = int(result[0])
        member_id = int(result[1])
        vote = int(result[2])
        people_rank =0.0
        if member_id in people_rank_dict:
            people_rank = people_rank_dict[member_id]
        if answer_id in hot_answers:
            hot_answers[answer_id] += vote * people_rank
        else:
            hot_answers[answer_id] = vote * people_rank

    #对编辑推荐的回答进行加权
    sql = 'SELECT count(*) FROM explore_archive where type = 1 and object_id = %s'
    for answer_id in hot_answers.keys():
        cursor.execute(sql, answer_id)
        result = cursor.fetchone()
        result_count = result[0]
        if result_count == 1:
            hot_answers[answer_id] += editor_value

    #对回答列表进行排序
    hot_answers = sorted(hot_answers.items(),key=itemgetter(1),reverse=True)
    cursor.execute("set names utf8")
    
    #获取热门回答的详细信息
    hot_answer_list = []
    for answer_id,vote_value in hot_answers:
        if vote_value < value_threshold:
            break
        sql = ("SELECT question_id,member_id,url_token,is_collapsed,content "
               "FROM answer WHERE id = %s")
        cursor.execute(sql, answer_id)
        result = cursor.fetchone()
        question_id = int(result[0])
        member_id = int(result[1])
        answer_token = int(result[2])
        is_collapsed = int(result[3])
        answer_content = result[4]
        if int(is_collapsed) != 0:
            continue

        sql = "SELECT count(*) FROM answer_vote WHERE answer_id = %s"
        cursor.execute(sql, answer_id)
        answer_vote = int(cursor.fetchone()[0])

        sql = "SELECT title,url_token FROM question WHERE id = %s"
        cursor.execute(sql, question_id)
        result = cursor.fetchone()
        question_title = result[0]
        question_token = result[1]
        question_url = 'http://www.zhihu.com/question/{0}'.format(question_token)
        answer_url = '{0}/answer/{1}'.format(question_url, answer_token)

        sql = "SELECT count(*) FROM anonymous WHERE member_id = %s and question_id = %s"
        cursor.execute(sql, (member_id, question_id))
        result = cursor.fetchone()[0]
        if (int(result) == 0):
            sql = "SELECT fullname,url_token,headline,avatar_path FROM member WHERE id = %s"
            cursor.execute(sql, member_id)
            result = cursor.fetchone()
            member_name = result[0]
            member_token = result[1]
            member_url = 'http://www.zhihu.com/people/{0}'.format(member_token)
            member_bio = result[2]
            member_avatar = result[3]
        else:
            member_name = '匿名用户'
            member_token = -1
            member_url = ''
            member_bio = ''
            member_avatar = ''

        sql = "SELECT topic_id FROM question_topic WHERE question_id = %s"
        cursor.execute(sql, question_id)
        flag = 0
        topic_list = []
        while (1):
            result = cursor.fetchone()
            if result == None:
                break
            topic_id = int(result[0])
            if topic_id in abandoned_topic_list:
                flag = 1
            topic_list.append(topic_id)
        if len(answer_content) < 150:
            flag = 1
        
        #组装
        answer = {'content':answer_content,
                  'id':answer_id,
                  'url':answer_url,
                  'vote':answer_vote}
        question = {'title':question_title,
                    'id':question_id,
                    'url':question_url}
        member = {'name':member_name,
                  'id':member_id,
                  'url':member_url,
                  'bio':member_bio,
                  'avatar':member_avatar}
        hot_answer = {'answer':answer,
                      'question':question,
                      'member':member,
                      'topic_list':topic_list,
                      'new_vote':vote_value}

        if flag == 0:
            hot_answer_list.append(hot_answer)
            output = '{0}\t{1}\t{2}\t{3}'.format(hot_answer['new_vote'],hot_answer['question']['title']
                                                ,hot_answer['member']['name'],hot_answer['answer']['url'])
            #print output
    
    print json.JSONEncoder().encode(hot_answer_list)
    #print len(hot_answer_list)

if __name__ == "__main__":
    main()
