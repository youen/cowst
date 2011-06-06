#!/usr/bin/python
# -*- encoding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

import subprocess
from logging import debug,info, warn, basicConfig, DEBUG,INFO
from itertools import groupby

#basicConfig(level=DEBUG)

basicConfig(level=INFO,format="%(asctime)s - %(levelname)s - %(message)s")
debug('load stream module')


class Formater():

    def __init__(self,template,alpha,histogram):
        self.template = template
        self.alpha = alpha
        self.histogram = histogram
    

    
    @property
    def histo(self):
        return '{' + ', '.join(map(lambda r:'(%d,%d)'%r,self.histogram.items())) +'}'
    
    def pig(self):
        return '%s\t%s\t%s'%(
                self.template,
                self.alpha,
                self.histo,
                )
class Stream():

    def __init__(self):
        self.children,self.parents = self.load_children_parents()





    def load_children_parents(self):

        info('loading children and parents from ship')
        f = open('subclassof')
        parents = {}
        children = {}
        for line in f:
            child , parent = line[:-1].split('\t')
            child = long(child)
            parent = long(parent)
            parents[child] = parents.get(child,set()) | set([parent])
            children[parent] = children.get(parent,set()) | set([child])

        info('%d parents relation loaded'%len(parents))
        info('%d children relation loaded'%len(children))
        return (children,parents)


    def stream_templates(self,stream):
        info('start stream properties')

        for template, histogram in groupby(stream,lambda x:long(x.split('\t',1)[0])):
            real_dom = {}
            for line in histogram:
                c , count = line[:-1].split('\t',3)[-2:]
                c= long(c)
                count = long(count)
                #print c,count , len(real_dom)
                real_dom[c] = count
            alpha = self.get_alpha(real_dom)
            yield Formater(
                    template,
                    alpha,
                    real_dom,
                    )

    def get_hierarchy(self,real_dom):

        def get_parents(c):
            return len(self.parents.get(c,[]))
        hierarchy = {}
        for c in real_dom.keys():
            parents = filter(lambda x:x!= c,self.parents.get(c,[None]))
            if len(parents) != 0:
                hierarchy[c] = max(parents,key=get_parents)
        return hierarchy

    def get_alpha(self,histogram):
        if len(histogram) == 0:
            return 0 #Nothing
        
        max_val = -1
        for c , val in histogram.items():

            if  max_val < val:
                alpha = c
                max_val = val

            elif max_val == val and alpha in self.parents.get(c,[]):
                alpha = c
        
        return alpha




        
        
if __name__ == '__main__':
    import sys 
    s = Stream()
    for c in s.stream_templates(sys.stdin):
        print c.pig()

