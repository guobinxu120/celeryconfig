from celery.decorators import task
from celery import Celery
from celery.exceptions import SoftTimeLimitExceeded
import celeryconfig
from celery.schedules import crontab
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
import requests, logging
import re
from requests.exceptions import ConnectionError
from celery_once import QueueOnce
import logging
import socket
from logging.handlers import SysLogHandler
import os
import urllib
from urllib.parse import urlparse
import validators

app = Celery()
app.config_from_object(celeryconfig)

api = "https://linkokay.com/"

app.conf.ONCE = {
  'backend': 'celery_once.backends.Redis',
  'settings': {
    'url': 'redis://127.0.0.1:6379/',
    'default_timeout': 60 * 10
  }
}

# with open('../config.json') as data_file:
#    data = json.load(data_file)

def urlUnshort(url, headers):
    if validators.url(url):
        try:
            if "t.umblr" in url:
                parsed = urlparse(url)
                url = urllib.parse.parse_qs(parsed.query)['z']
                return url[0]
            response = requests.get(url, headers=headers, timeout=5, allow_redirects=True,)
            if response.history:
                return response.url
            else:
                return False
        except ConnectionError as e:
            return False


@app.task(base=QueueOnce, once={'graceful': True})
def website_crawler(url, target, id):
    print('website_crawler',url,target,id)
    headers = {
        'User-Agent': 'Googlebot/2.1 (+http://www.googlebot.com/bot.html)',
        'Accept': '*/*'
    }
    try:
        rew = requests.get(url.strip(), headers=headers, verify=False, stream=True, timeout=5)
        ip = rew.raw._fp.fp.raw._sock.getpeername()
        link_type = ""
        if rew.status_code == 200:
            soup = BeautifulSoup(rew.content, "html.parser")
            pattern = r"(http|https):\/\/(www.|)" + re.escape(target) + r"($|[^.])"
            patter = re.compile(pattern, re.IGNORECASE)
            flag = 1

            # OBL Check
            urlv = urlparse(url)
            extp = r"https?:\/\/(?!" + re.escape(urlv.netloc) + ")"
            obls = soup.findAll('a', href=re.compile(extp, re.IGNORECASE))
            for anchor in soup.findAll('a', href=True):
                if patter.match(anchor['href']):
                    flag = 2
                    anchortext = anchor.text
                    rel = 'dofollow'
                    isImage = False
                    if anchor.img:
                        isImage = True
                        if 'alt' in anchor.img:
                            anchortext = anchor.img['alt']
                            link_type = "image"
                        else:
                            anchortext = anchor['href']
                            link_type = "text"
                    if anchor.has_attr('rel'):
                        if 'nofollow' in anchor['rel']:
                            rel = 'nofollow'
                    req = requests.post('https://app.linkokay.com/api/backlinks',
                                        data={'id': id,
                                              'anchor': anchortext,
                                              'status_code': rew.status_code,
                                              'present': True, 'rel': rel,
                                              'obl': len(obls), 'ip': ip[0],
                                              'image': isImage
                                              })
                    return 'Match Found' + str(req.status_code)
            if flag == 1:
                req = requests.post('https://app.linkokay.com/api/backlinks',
                                    data={'id': id, 'anchor': None, 'status_code': rew.status_code, 'present': False,
                                          'obl': len(obls), 'ip': ip[0]})
                # redirect_crawler.delay(url, target, id)
                redirect_crawler(url, target, id)
        else:
            req = requests.post('https://app.linkokay.com/api/backlinks',
                                data={'id': id, 'anchor': None, 'status_code': rew.status_code, 'present': False})
            return 'Server Error' + str(req.status_code)
    except ConnectionError as e:  # This is the correct syntax
        req = requests.post('https://app.linkokay.com/api/backlinks',
                            data={'id': id, 'anchor': None, 'status_code': 000, 'present': False})
        return 'Connection Error' + str(req.status_code)
    except requests.exceptions.Timeout as err:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36',
            'Accept': '*/*'
        }
        rew = requests.get(url.strip(), headers=headers, verify=False, stream=True)
        ip = rew.raw._fp.fp.raw._sock.getpeername()
        if rew.status_code == 200:
            soup = BeautifulSoup(rew.content,
                                 "html.parser")
            pattern = r"(http|https):\/\/(www.|)" + re.escape(target) + r"($|[^.])"
            patter = re.compile(pattern, re.IGNORECASE)
            flag = 1

            # OBL Check
            urlv = urlparse(url)
            extp = r"https?:\/\/(?!" + re.escape(urlv.netloc) + ")"
            obls = soup.findAll('a', href=re.compile(extp, re.IGNORECASE))
            for anchor in soup.findAll('a', href=True):
                if patter.match(anchor['href']):
                    flag = 2
                    anchortext = anchor.text
                    rel = 'dofollow'
                    if anchor.img:
                        if 'alt' in anchor.img:
                            anchortext = anchor.img['alt']
                        else:
                            anchortext = anchor['href']
                    if anchor.has_attr('rel'):
                        if 'nofollow' in anchor['rel']:
                            rel = 'nofollow'
                    req = requests.post('https://app.linkokay.com/api/backlinks',
                                        data={'id': id, 'anchor': anchortext, 'status_code': rew.status_code,
                                              'present': True, 'rel': rel, 'obl': len(obls), 'ip': ip[0]})
                    return 'Match Found' + str(req.status_code)
            if flag == 1:
                req = requests.post('https://app.linkokay.com/api/backlinks',
                                    data={'id': id, 'anchor': None, 'status_code': rew.status_code, 'present': False,
                                          'obl': len(obls), 'ip': ip[0]})
        else:
            req = requests.post('https://app.linkokay.com/api/backlinks',
                                data={'id': id, 'anchor': None, 'status_code': rew.status_code, 'present': False})
            return 'Server Error' + str(req.status_code)
    except SoftTimeLimitExceeded as e:
        req = requests.post('https://app.linkokay.com/api/backlinks',
                            data={'id': id, 'anchor': None, 'status_code': 000, 'present': False})
    except:
        req = requests.post('https://app.linkokay.com/api/backlinks',
                            data={'id': id, 'anchor': None, 'status_code': 000, 'present': False})


@app.task
def backlink_update(id, anchor, status, present):
    print('backlink_update', id, anchor, status, present)
    r = requests.post('https://app.linkokay.com/api/backlinks',
                      data={'id': id, 'anchor': anchor, 'status_code': status, 'present': present})
    return 0


# @app.task(base=QueueOnce, once={'graceful': True})
def redirect_crawler(url, target, id):
    print('redirect_crawler', url, target, id)
    headers = {
        'User-Agent': 'Googlebot/2.1 (+http://www.googlebot.com/bot.html)',
        'Accept': '*/*'
    }
    try:
        rew = requests.get(url.strip(), headers=headers, verify=False, stream=True, timeout=5)
        ip = rew.raw._fp.fp.raw._sock.getpeername()
        link_type = ""
        if rew.status_code == 200:
            soup = BeautifulSoup(rew.content, "html.parser")
            pattern = r"(http|https):\/\/(www.|)" + re.escape(target) + r"($|[^.])"
            patter = re.compile(pattern, re.IGNORECASE)
            flag = 1

            # OBL Check
            urlv = urlparse(url)
            extp = r"https?:\/\/(?!" + re.escape(urlv.netloc) + ")"
            obls = soup.findAll('a', href=re.compile(extp, re.IGNORECASE))
            for anchor in soup.findAll('a', href=True):
                redirectURL = urlUnshort(anchor['href'], headers)
                if redirectURL:
                    if patter.match(redirectURL):
                        flag = 2
                        anchortext = anchor.text
                        rel = 'dofollow'
                        isImage = False
                        if anchor.img:
                            isImage = True
                            if 'alt' in anchor.img:
                                anchortext = anchor.img['alt']
                                link_type = "image"
                            else:
                                anchortext = anchor['href']
                                link_type = "text"
                        if anchor.has_attr('rel'):
                            if 'nofollow' in anchor['rel']:
                                rel = 'nofollow'
                        req = requests.post('https://app.linkokay.com/api/backlinks',
                                            data={'id': id, 'anchor': anchortext,
                                                  'status_code': rew.status_code,
                                                  'present': True, 'rel': rel,
                                                  'obl': len(obls), 'ip': ip[0],
                                                  'redirect': True, 'image': isImage})
                        return 'Match Found Redirect ' + str(req.status_code)
            if flag == 1:
                req = requests.post('https://app.linkokay.com/api/backlinks',
                                    data={'id': id, 'anchor': None, 'status_code': rew.status_code, 'present': False,
                                          'obl': len(obls), 'ip': ip[0]})
                return 'No Match Found' + str(req.status_code)
        else:
            req = requests.post('https://app.linkokay.com/api/backlinks',
                                data={'id': id, 'anchor': None, 'status_code': rew.status_code, 'present': False})
            return 'Server Error' + str(req.status_code)
    except ConnectionError as e:  # This is the correct syntax
        req = requests.post('https://app.linkokay.com/api/backlinks',
                            data={'id': id, 'anchor': None, 'status_code': 000, 'present': False})
        return 'Connection Error' + str(req.status_code)
    except SoftTimeLimitExceeded as e:
        req = requests.post('https://app.linkokay.com/api/backlinks',
                            data={'id': id, 'anchor': None, 'status_code': 000, 'present': False})
    except:
        req = requests.post('https://app.linkokay.com/api/backlinks',
                            data={'id': id, 'anchor': None, 'status_code': 000, 'present': False})


@app.task(base=QueueOnce, once={'graceful': True})
def webcrawl_400(url, target, id):
    print('webcrawl_400', url, target, id)
    headers = {
        'User-Agent': 'Googlebot/2.1 (+http://www.googlebot.com/bot.html)',
        'Accept': '*/*'}
    try:
        rew = requests.get(url.strip(), headers=headers, verify=False)
        if rew.status_code == 200:
            soup = BeautifulSoup(rew.content, "html.parser")
            soup.prettify()
            pattern = r"(http|https):\/\/(www.|)" + re.escape(target) + r"($|[^.])"
            patter = re.compile(pattern, re.IGNORECASE)
            flag = 1

            # OBL Check
            urlv = urlparse(url)
            extp = r"https?:\/\/(?!" + re.escape(urlv.netloc) + ")"
            obls = soup.findAll('a', href=re.compile(extp, re.IGNORECASE))

            for anchor in soup.findAll('a', href=True):
                if patter.match(anchor['href']):
                    flag = 2
                    anchortext = anchor.text
                    rel = 'dofollow'
                    if anchor.img:
                        if 'alt' in anchor.img:
                            anchortext = anchor.img['alt']
                        else:
                            anchortext = anchor['href']
                    if anchor.has_attr('rel'):
                        if 'nofollow' in anchor['rel']:
                            rel = 'nofollow'
                    requests.post('https://app.linkokay.com/api/backlinks',
                                  data={'id': id, 'anchor': anchortext, 'status_code': rew.status_code,
                                        'present': True, 'rel': rel, 'obl': len(obls)})
                    return 'Match Found'
            if flag == 1:
                requests.post('https://app.linkokay.com/api/backlinks',
                              data={'id': id, 'anchor': None, 'status_code': rew.status_code, 'present': False,
                                    'obl': len(obls)})
                return 'No Match Found'
        else:
            requests.post('https://app.linkokay.com/api/backlinks',
                          data={'id': id, 'anchor': None, 'status_code': rew.status_code, 'present': False})
            return 'Server Error'
    except ConnectionError as e:  # This is the correct syntax
        requests.post('https://app.linkokay.com/api/backlinks',
                      data={'id': id, 'anchor': None, 'status_code': 000, 'present': False})
    except SoftTimeLimitExceeded as e:
        req = requests.post('https://app.linkokay.com/api/backlinks',
                            data={'id': id, 'anchor': None, 'status_code': 000, 'present': False})
    except:
        req = requests.post('https://app.linkokay.com/api/backlinks',
                            data={'id': id, 'anchor': None, 'status_code': 000, 'present': False})
