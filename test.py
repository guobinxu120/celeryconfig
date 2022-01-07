from celery import Celery
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

url = 'http://amandoblogs.com/5-kinds-of-tea-to-give-your-health-a-boost/'
url = 'http://www.mrhealthylife.com/forget-coffee-this-is-the-newest-healthiest-alternative-to-start-your-day-fresh/'
target = 'health.com/nutrition/what-is-matcha'

headers = {
    'User-Agent': 'Googlebot/2.1 (+http://www.googlebot.com/bot.html)',
    'Accept': '*/*'
}
try:
    rew = requests.get(url.strip(), headers=headers, verify=False, stream=True, timeout=30)
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
                print('Match Found')
        if flag == 1:
            print('here')
    else:
        print('here 2')
except ConnectionError as e:  # This is the correct syntax
    print('here 3')
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
                print('here 4')
        if flag == 1:
            print('here 5')

    else:
        print('here6')
