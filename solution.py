#!/usr/bin/env python3

import psycopg2

def generate_report():
    print(find_most_readed_articles())
    print(find_most_readed_authors())

def find_most_readed_articles():
    db = psycopg2.connect(database="news")
    c = db.cursor()
    query = '''
        select a.title, count(*) from log l
            join articles a on concat('/article/', a.slug) = l.path
            group by a.title
            order by count desc
    '''
    c.execute(query)
    result = c.fetchall()
    db.close()
    return result

def find_most_readed_authors():
    db = psycopg2.connect(database="news")
    c = db.cursor()
    query = '''
        select au.name as author, count(*) from log l
            join articles ar on concat('/article/', ar.slug) = l.path
            join authors au on ar.author = au.id
            group by au.name
            order by count desc
    '''
    c.execute(query)
    result = c.fetchall()
    db.close()
    return result

def find_most_request_errors():
    db = psycopg2.connect(database="news")
    c = db.cursor()
    query = '''
        select au.name as author, count(*) from log l
            join articles ar on concat('/article/', ar.slug) = l.path
            join authors au on ar.author = au.id
            group by au.name
            order by count desc
    '''
    c.execute(query)
    result = c.fetchall()
    db.close()
    return result

if __name__ == '__main__':
    generate_report()