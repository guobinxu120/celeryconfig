from flask import Flask, request
from tasks import website_crawler

app = Flask(__name__)


@app.route('/website_crawler', methods=['POST'])
def run():
    link = request.form['link']
    target = request.form['target']
    id = request.form['id']
    print (link, target, id)
    website_crawler.delay(link, target, id)
    return 'Hello'


if __name__ == '__main__':
    app.run()
