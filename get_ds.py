#!/usr/bin/env python3
import hashlib
import json
import logging
import os.path
import pandas as pd
import requests
import ssl
import urllib
import urllib.error
from urllib.request import urlretrieve
from progress.bar import Bar
from progress.spinner import Spinner

DATASET_LIST = './data/datasets.json'
DATASRC_DIR = './data/src'
DATASET_DIR = './data/raw'

HEADERS = {
    'user-agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
}

# logger
LOG_FORMAT = '%(levelname)s\t%(asctime)s:\t%(message)s'
logging.basicConfig(level = logging.DEBUG, format = LOG_FORMAT)
log = logging.getLogger(__name__)

class Fetcher:

    def check_hash(self, filename, dhash='md5'):
        '''Compute file hash
        '''
        # BUF_SIZE is totally arbitrary, change for your app!
        BUF_SIZE = 65536  # lets read stuff in 64kb chunks!

        if dhash == 'md5':
            fhash = hashlib.md5()
        elif dhash == 'sha1':
            fhash = hashlib.sha1()
        else:
            return -1

        with open(filename, 'rb') as f:
            while True:
                data = f.read(BUF_SIZE)
                if not data:
                    break
                fhash.update(data)

        return fhash.hexdigest()

    def get(self, url, fname, verify=False):
        '''Download file from url (using requests, then urllib in case of error)

        Parameters
        ----------
            url | string

            fname | str (optional)
                Destination filename. If not defined, original name will be used

            verify | Boolean
                Check SSL Certificate
        '''
        try:
            downloaded_fname = self.get_urllib(url, fname)
            if not downloaded_fname:
                log.warning(
                    f'Unable to download "{url}" using urllib . Trying requests lib'
                )
                raise Exception("Unable to download using urllib")

        except Exception:

            try:
                fname = self.get_requests(url, fname, verify=verify)

            except Exception as e2:
                log.warning(
                    f'Unable to download using requests: {e2}. Giving up!'
                )
                fname = None

        return fname

    def get_requests(self, url, fname=None, verify=False):
        '''Download file from url using requests library
        '''
        r = requests.get(
            url, stream=True, verify=verify, headers=HEADERS
        )
        size = r.headers['content-length']
        if not fname:
            fname = url.split('/')[-1]

        if size:
            p = Bar(fname, max=int(size))
        else:
            p = Spinner(fname)

        with open(fname, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024*50):
                if chunk: # filter out keep-alive new chunks
                    p.next(len(chunk))
                    f.write(chunk)

        p.finish()
        return fname

    def get_urllib(self, url, fname):
        '''Download file from url using urllib (works for ftp urls)
        '''
        self.p = None

        def update(blocks, bs, size):
            if not self.p:
                if size < 0:
                    self.p = Spinner(fname)
                else:
                    self.p = Bar(fname, max=size)
            else:
                if size < 0:
                    self.p.update()
                else:
                    self.p.goto(blocks * bs)

        try:
            ssl._create_default_https_context = ssl._create_unverified_context
            urlretrieve(url, fname, update)
        except ssl.SSLCertVerificationError or ssl.SSLError:
            ssl._create_default_https_context = ssl._create_unverified_context
            urlretrieve(url, fname, update)
        except urllib.error.HTTPError as e:
            log.error(f'ERR: {e.code} {e.reason}. {url}\n')
            return
        except urllib.error.URLError as e:
            try: # Try without certificate check
                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                urlretrieve(url, fname, update)
            except Exception as e:
                log.error(f'\t{e}. {url}\n')
            return

        # self.p.finish()
        return fname

def read_json(filename):

    datasets = None

    # read articles from json
    with open(filename, 'r') as f:
        datasets = json.load(f)['datasets']
        log.info(f'--> {len(datasets)} datasets found in from "{filename}"')

    return datasets

def download_resource(url, path, fname, resource_hash):
    """Download resource
    """

    if not os.path.exists(path):
        os.makedirs(path)

    # File exists
    if os.path.isfile(fname):

        fhash = Fetcher().check_hash(fname)

        # Corrupted, downloading again
        if fhash != resource_hash:
            log.warning(f'\tArquivo {fname} corrompido! Baixando.novamente.')
            Fetcher().get(url, fname)

        # Not-corrupted, skiping
        else:
            log.info(f'\tArquivo {fname} íntegro. Download ignorado.')

    # File don't exist, downloading
    else:
        log.info( f'\tArquivo {fname} não localizado. Iniciando Download.')
        Fetcher().get(url, fname)

### Main
if __name__ == '__main__':

    # Read datasets.json
    try:
        datasets = read_json(DATASET_LIST)

    except Exception as e:
        log.error(f'Unable to read datasets. {e}')
        exit(-1)

    # Process datasets
    try:

        # Iterate over datasets
        for ds in datasets:

            # Iterate over dataset resources
            dsname = ds['id']

            if dsname == '':
                pass

            else:
                log.info(f'--> {ds["id"]}: {ds["name"]}')
                path = DATASRC_DIR + '/' + dsname.lower()
                for resource in ds['resources']:
                    if resource['url'] == '':
                        pass
                    else:
                        log.info(f'\t"{resource["filename"]}"')
                        fname = path + '/' + resource['filename']
                        download_resource(
                            resource['url'], path, fname, resource['hash']
                        )

    except Exception as e:
        log.warning(f'Unable to process dataset "{ds["id"]}". {e}')

        pass
