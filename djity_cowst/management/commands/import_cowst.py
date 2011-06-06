# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
import gzip
import sys

from subprocess import  PIPE,Popen

from itertools import imap, groupby

from django.core.management.base import BaseCommand, CommandError

from django.db import connection, transaction
from djity_cowst.models import YagoClass, InfoboxProperty , InfoboxPropertyHistogram, Dico, Instance
import djity_cowst
mappingbassed_properties_list = djity_cowst.__path__[0] + '/data/mappingbased_properties_list.tsv.gz'

infobox_properties_histogram = djity_cowst.__path__[0] + '/data/infobox_properties_histogram.tsv.gz'


class Chunk:
    def __init__(self,iterator,chunksize):
        self.index = 0
        self.iterator = iterator
        self.chunksize = chunksize

    def __iter__(self):
        return self

    def next(self):
        if self.index < self.chunksize:
            self.index  += 1
            return self.iterator.next()
        else:
            raise StopIteration

def chunkify(iterator,chunksize = 1000, start = 0):
    for i in range(start):
        iterator.next()

    chunk = None
    while chunk == None  or chunk.index ==  chunksize :
        del chunk
        chunk = Chunk(iterator,chunksize)
        yield chunk
        print chunk.index

    

def hdfs_open(path):
    p = Popen(['hadoop','fs','-cat','%s/par*/'%path],
            stdout =PIPE
            )


    return p.stdout


class Command(BaseCommand):

    def import_infobox_properties(self):
        def parse_tuple(line):
            return tuple(map(long,line.split('\t',4)[:4]))
        
        def key(args):
            return args[:3]



        f = hdfs_open('swoct/dbpedia/infobox_properties_histogram')
        i =0
        for k,g in groupby(imap(parse_tuple,f),key=key):
            print i
            i += 1
            with transaction.commit_on_success():
                prop = InfoboxProperty(md5 = k[0])
                prop.save()
                yago = YagoClass(md5=k[1])
                yago.save()
                h= InfoboxPropertyHistogram( infobox_property=prop, count =k[2], yago_class=yago)
                h.save()
                for sample in g:
                    h.sample.add(Instance.objects.get(md5=sample[3]))


    def import_instances(self):
        f = hdfs_open('swoct/dbpedia/instances_tsv')
        for i,chunk in enumerate(chunkify(f)):
            with transaction.commit_on_success():
                for line in chunk:
                    code , rank= line.split('\t',2)[:2]
                    code = long(code)
                    rank = float(rank)
                    ins = Instance(md5=code,rank=rank)
                    ins.save()

            print "%d instances imported"%((i+1)*1000)
        f.close()


    def import_dico(self):
        f = hdfs_open('swoct/dbpedia/dico')
        for i,chunk in enumerate(chunkify(f)):
            with transaction.commit_on_success():
                for line in chunk:
                    code , uri= line.split('\t',2)[:2]
                    code = long(code)
                    uri = uri.strip()
                    if len(uri) > 255:
                        continue
                    dico = Dico(code=code,uri=uri)
                    dico.save()

            print "%d dico entry  imported"%(i*1000)
        f.close()

    def import_yago_classes(self):

        f = gzip.open(djity_cowst.__path__[0] + '/data/yago_classes_list.tsv.gz')
        for i,chunk in enumerate(chunkify(f)):
            with transaction.commit_on_success():
                for line in chunk:
                    
                    md5_code = long(line.rstrip())
                    yago = YagoClass(md5=md5_code)
                    yago.save()
            print "%d yago class imported"%(i*1000)
        

    def import_infobox_properties_histogram(self):
        f = open(djity_cowst.__path__[0] + '/data/infobox_properties_histogram.tsv')
        for i,chunk in enumerate(chunkify(f,10000)):
            with transaction.commit_on_success():
                for j,line in enumerate(chunk):
                    props , yagos, count = line.split('\t',3)[:3]
                    propl = long(props)
                    yagol = long(yagos)
                    count = int(count)
                    prop = InfoboxProperty(md5 = propl)
                    yago = YagoClass(md5=yagol)
                    h= InfoboxPropertyHistogram( infobox_property=prop, count =count, yago_class=yago)
                    h.save()
                    del h
                    del propl
                    del yagol
                    del props
                    del yagos
                    del prop
                    del yago
                    del count
                    del line

            print "%d infobox properties relations imported"%((i+1)*10000)






    def handle(self,*args,**options):
        
        if raw_input('import instances ?') in ('y','yes'):
            self.import_instances()

        if raw_input('import dico ?') in ('y','yes'):
            self.import_dico()


        if raw_input('import yago class ?') in ('y','yes',''):
            self.import_yago_classes()

        if raw_input('import infobox properties ?') in ('y','yes',''):
            self.import_infobox_properties()


        if raw_input('import infobox properties histograms?') in ('y','yes',''):
            self.import_infobox_properties_histogram()

