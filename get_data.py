import io
import os
import sys
import zipfile
import datetime
from xml.dom.minidom import parseString as parseXMLString

import requests
from tqdm import tqdm

class Downloader:
    def __init__(self, outPath):
        self.getDays    = 90
        self.getTo      = datetime.datetime.now(datetime.timezone.utc).date() - datetime.timedelta(days=1)
        self.getFrom    = self.getTo - datetime.timedelta(days=self.getDays)
        self.urlBaseS3  = 's3-ap-northeast-1.amazonaws.com'
        self.urlBaseBV  = 'data.binance.vision'
        self.urlDataTop = f'https://{self.urlBaseS3}/data.binance.vision?delimiter=/&prefix=data/futures/um/daily/klines/'
        self.outPath    = outPath
        self.outHeader  = 'open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore'
        self.symbols    = set()
        self.s          = requests.Session()

    def populateSymbols(self):
        r = self.s.get(self.urlDataTop).text
        doc = parseXMLString(r)

        for el in doc.getElementsByTagName('Prefix'):
            elText = el.childNodes[0]
            parts = elText.data.split('/')
            if len(parts) == 7 and parts[4] == 'klines' and parts[5].endswith('USDT'):
                self.symbols.add(parts[5])

    def checksumUrl(self, symbol, date):
        dateString = date.strftime('%Y-%m-%d')
        return f'https://{self.urlBaseBV}/data/futures/um/daily/klines/{symbol}/1m/{symbol}-1m-{dateString}.zip.CHECKSUM'

    def checkAvailabilityForURL(self, url):
        return self.s.get(url).status_code == 200

    def checkAvailabilityForSymbol(self, symbol):
        return self.checkAvailabilityForURL(self.checksumUrl(symbol, self.getFrom)) and self.checkAvailabilityForURL(self.checksumUrl(symbol, self.getTo))

    def symbolUrl(self, symbol, date):
        dateString = date.strftime('%Y-%m-%d')
        return f'https://{self.urlBaseBV}/data/futures/um/daily/klines/{symbol}/1m/{symbol}-1m-{dateString}.zip'

    def dateRange(self, start, stop):
        datesToGet = []
        while start <= stop:
            datesToGet.append(start)
            start += datetime.timedelta(days=1)
        return datesToGet

    def getSymbol(self, symbol):
        fnOf = os.path.join(self.outPath, symbol + '.csv')
        fileMode = 'w'
        startDate = self.getFrom
        if os.path.exists(fnOf):
            # we already have data, so we will append and only fetch new data
            fileMode = 'a'
            startDate = self.lastDate(fnOf) + datetime.timedelta(days=1)
        datesToGet = self.dateRange(startDate, self.getTo)
        if len(datesToGet) > 0:
            print('getting %s days of data for %s...' % (len(datesToGet), symbol))
            with open(fnOf, fileMode) as of:
                if fileMode == 'w':
                    of.write(self.outHeader + '\n')
                for thisDate in tqdm(datesToGet, desc=symbol):
                    dateString = thisDate.strftime('%Y-%m-%d')
                    b = self.s.get(f'https://{self.urlBaseBV}/data/futures/um/daily/klines/{symbol}/1m/{symbol}-1m-{dateString}.zip', timeout=10)
                    with zipfile.ZipFile(io.BytesIO(b.content)) as zf:
                        fn = zf.infolist()[0].filename
                        zfData = zf.read(fn)
                        zfDataLines = zfData.decode('utf-8').rstrip().split('\n')
                        while not zfDataLines[0][0].isnumeric(): # get rid of header
                            zfDataLines.pop(0)
                        for line in zfDataLines:
                            of.write(line + '\n')

    def getAll(self):
        self.populateSymbols()
        print('pruning symbols...')
        self.symbols = set([ x for x in tqdm(self.symbols) if self.checkAvailabilityForSymbol(x) ])
        print('downloading data for %s symbols...' % len(self.symbols))
        for symbol in sorted(self.symbols):
            self.getSymbol(symbol)

    def lastLine(self, fn):
        ll = None
        try:
            with open(fn) as f:
                while True:
                    ll = next(f)
        except StopIteration:
            return ll

    def lastDate(self, fn):
        ll = self.lastLine(fn)
        lastEpoch = float(ll.split(',', 1)[0]) / 1000
        return datetime.date.fromtimestamp(lastEpoch)

if __name__ == '__main__':
    d = Downloader('data' if len(sys.argv) == 1 else sys.argv[1])
    d.getAll()
