import urllib.request
import codecs
import json
import os
import datetime
import decorator
import shelve
import time
import os
from hashlib import md5

serviceprefix = os.environ['SRV_PREFIX']
service = "modifiedsince.aspx?f=%s&e=%s&d=%s"
expression = os.environ['SRV_FILTER']
folder = ""
if 'SRV_FOLDER' in os.environ:
    folder = os.environ['SRV_FOLDER']
since = ""
logstash = "logstash"
retry = []
serviceurl = serviceprefix + (service % (folder, expression, "%s"))


def scached(cache_file, expiry):
    """ Decorator setup """

    def scached_closure(func, *args, **kw):
        """ The actual decorator """
        key = md5(':'.join([func.__name__, str(args), str(kw)]).encode('utf-8')).hexdigest()
        d = shelve.open(cache_file)

        # Expire old data if we have to
        if key in d:
            if d[key]['expires_on'] < datetime.datetime.now():
                del d[key]

        # Get new data if we have to
        if key not in d:
            data = func(*args, **kw)
            d[key] = {
                'expires_on' : datetime.datetime.now() + expiry,
                'data': data,
            }

        # Return what we got
        result = d[key]['data']
        d.close()

        return result

    return decorator.decorator(scached_closure)


import re
valid = re.compile(r"^(?:\s+|)<\?xml.+>(?:\s+|)$", re.DOTALL)
@scached(cache_file='lastfiles.db', expiry=datetime.timedelta(minutes=60))
def getFile(filePath, since):
    reader = codecs.getreader("latin_1")
    req = urllib.request.urlopen(filePath)
    uencodeddata = req.read().decode('latin_1')
    encodeddata = uencodeddata
    #print (encodeddata)
    if valid.match(encodeddata) is None:
        raise ValueError('Server didn\'t return a valid xml at' + filePath)
    return encodeddata

cache = shelve.open('puller.cache')

while True:
    try:
        if 'since' in cache:
            since = cache['since']

        if 'retry' in cache:
            retry = cache['retry']

        #print url.encode('utf-8')
        timedservice = serviceurl % since
        print(timedservice)
        reader = codecs.getreader("utf-8")
        req = urllib.request.urlopen(timedservice)
        data = json.load(reader(req))

        print(data["filter"])
        print(data["timestamp"])
        data["files"].extend(retry)
        retry = []

        withErrors = False
        for item in data["files"]:
            filePath = serviceprefix
            if folder != "":
                filePath = filePath + folder + '/'
            filePath = filePath + item

            print(filePath)
            try:
                decodeddata = getFile(filePath, since)

                urllib.request.urlopen('http://' + logstash + ':8080', data=decodeddata.encode())
            except ValueError as err:
                print(err.args)
                retry.append(item)
                withErrors = True

        cache['since'] = data["timestamp"]
    except Exception as err:
        print(err)
        withErrors = True

    if withErrors:
      	print("That wasn\'t perfect...")

    cache.sync()
    time.sleep(5)

