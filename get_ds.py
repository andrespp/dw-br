#!/usr/bin/env python3
import os.path
import hashlib
import requests
import pandas as pd
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
        r = requests.get(url, stream=True)
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

        urlretrieve(url, to, update)
        self.p.finish()


### Main
if __name__ == '__main__':

    df = pd.read_csv(DATASET_LIST)
    #df = df.head(5)

    for index, ds in df.iterrows():
        fname = DATASRC_DIR + '/' + ds['arquivo']

        if os.path.isfile(fname):
            fhash = Fetcher().check_hash(fname)
            if fhash != ds['hash_md5']:
                print('WARN: Arquivo {} corrompido! Baixando.'
                      'novamente '.format(fname), end='')
                Fetcher().get_urllib(ds['url'], fname)
            else:
                print('Arquivo {} íntegro. Download ignorado.'.format(fname))
        else:
            print('Arquivo {} não localizado. '
                  'Iniciando Download. '.format(fname), end='')
            Fetcher().get_urllib(ds['url'], fname)

