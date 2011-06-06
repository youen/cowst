#!/usr/bin/python

from subprocess import call
import sys

from logging import info,error

if __name__ == '__main__':

    name = sys.argv[1]
    output = sys.argv[2]
    inputs = dict(map(lambda x:tuple(x[1:].split('=')), filter(lambda x: x[0] == 'i', sys.argv[2:])))
    outputs = dict(map(lambda x:tuple(x[1:].split('=')), filter(lambda x:x[0] == 'o', sys.argv[2:])))


    ipd = inputs['pig_subclassof_md5']
    opd = outputs['subclassof']
    info('copy %s to local at %s'%(ipd,opd))
    if 0 != call('hadoop fs -cat ' + ipd + '/par* >'+ opd,shell=True):
        error("can't copy to local")
        exit(2)

    ipd = inputs['dico']
    opd = outputs['dico']
    info('copy %s to local at %s'%(ipd,opd))
    if 0 != call('hadoop fs -cat ' + ipd + '/par* >'+ opd,shell=True):
        error("can't copy to local")
        exit(2)
