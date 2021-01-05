#!/usr/bin/env python3
import ssl
import os.path
import hashlib
import requests
import pandas as pd
import urllib
from urllib.request import urlretrieve
from progress.bar import Bar
from progress.spinner import Spinner

DATASRC_DIR = './datasrc'
DATASET_DIR = './dataset'
DATASET_LIST = './datasets.csv'

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

    def get(self, url, fname=False):
        '''Download file from url using requests library
        '''
        r = requests.get(url, stream=True, verify=False)
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

    def get_urllib(self, url, to):
        '''Download file from url using urllib (works for ftp urls)
        '''
        self.p = None

        def update(blocks, bs, size):
            if not self.p:
                if size < 0:
                    self.p = Spinner(to)
                else:
                    self.p = Bar(to, max=size)
            else:
                if size < 0:
                    self.p.update()
                else:
                    self.p.goto(blocks * bs)

        try:
            ssl._create_default_https_context = ssl._create_unverified_context
            urlretrieve(url, to, update)
        except ssl.SSLCertVerificationError or ssl.SSLError:
            ssl._create_default_https_context = ssl._create_unverified_context
            urlretrieve(url, to, update)
        except urllib.error.HTTPError as e:
            print(f'ERR: {e.code} {e.reason}. {url}\n', flush=True)
            return
        except urllib.error.URLError as e:
            print(f'ERR: {e.reason}. {url}\n', flush=True)
            return

        self.p.finish()


### Main
if __name__ == '__main__':

    df = pd.read_csv(DATASET_LIST)
    df['download'] = df['download'].apply(lambda x:
                                          True if x.upper()=='S' else False)

    for index, ds in df[df['download']].iterrows():
        fname = DATASRC_DIR + '/' + ds['arquivo']

        if os.path.isfile(fname):
            fhash = Fetcher().check_hash(fname)
            if fhash != ds['hash_md5']:
                print(f'WARN: Arquivo {ds["id"]}-{fname} corrompido! Baixando.'
                      'novamente ', flush=True)
                Fetcher().get_urllib(ds['url'], fname)
            else:
                print(f'Arquivo {ds["id"]}-{fname} íntegro. Download ignorado.')
        else:
            print(f'Arquivo {ds["id"]}-{fname} não localizado. '
                   'Iniciando Download.', flush=True)
            Fetcher().get_urllib(ds['url'], fname)

