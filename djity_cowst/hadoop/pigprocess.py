# -*- encoding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

from logging import debug,info,warn,error
from subprocess import call, PIPE,Popen
import os
from rdflib import RDFS

from datetime import datetime

hdfs_dir = '/user/yperon/swoct/'
local_dir = 'bench/'

cache_hdfs_timestamp = {}

def pigTimeStamp(file_name):
    if   file_name in cache_hdfs_timestamp.keys():
        return cache_hdfs_timestamp[file_name]
    else:
        debug('get time stamp of %s from hdfs'%file_name)
        try:
            p = Popen(['hadoop','fs','-ls',file_name],
                    stdin = PIPE,
                    stdout= PIPE,
                    stderr=PIPE
                    )
            out,err = p.communicate()
            out = out.split('\n',2)[1]
            out = out.split(' ')
            out =  filter(lambda x:x!='',out)
            datestr = '%s %s'%(out[5],out[6])
            cache_hdfs_timestamp[file_name] = datetime.strptime(datestr,'%Y-%m-%d %H:%M')
            return cache_hdfs_timestamp[file_name]
        except :
            cache_hdfs_timestamp[file_name] = datetime.strptime('','')
            return cache_hdfs_timestamp[file_name]

class Process(object):

    def __init__(self,name):
        self.name = name
    
    def run(self):
            inputs = map(lambda (x,y): 'i%s=%s'%(x,y),self.inputs.items())
            outputs = map(lambda (x,y): 'o%s=%s'%(x,y),self.outputs.items())
            info('launch `%s`'%' '.join([self.script,self.name] + outputs + inputs  ))
            retcode = call(['./'+self.script,self.name] + outputs + inputs  )
            if retcode != 0 :
                error("script %s don't finish with status 0"%self.script)
                return False

            return True

    @property
    def dependences(self):
        return map(lambda x:x(self.name),self.dependencesProcess)
    
    @property
    def uptodate(self):

        if self.script_timestamp > self.outputs_timestamp:
            info('script %s was changed and was not applied'%self.script)
            info('script %s was changed and was not applied'%self.script)
            debug('sript :  %s  , output : %s '%(self.script_timestamp.isoformat(), self.outputs_timestamp.isoformat()))
            return False

        if self.outputs_timestamp < self.inputs_timestamp:
            info('input of %s changed'%self.__class__.__name__)
            debug('inputs :  %s  , outputs : %s '%(self.inputs_timestamp.isoformat(), self.outputs_timestamp.isoformat()))
            return False
    
        for dep in self.dependences :
            if not dep.uptodate :
                return False

        info('process %s is up-to-date'%self.__class__.__name__)
        return True

    def timestamp(self,filename):
        try:
            return datetime.fromtimestamp(os.stat(filename).st_mtime)
        except:
            return datetime.strptime('','')

    @property
    def script_timestamp(self):
        return self.timestamp(self.script)

    def start(self,dry=False):
        if not self.uptodate:
            for dep in self.dependences:
                if not dep.start(dry=dry):
                    return False
            info('run %s'%self.__class__.__name__)
            if dry: return True
            return self.run()
        return True

    @property
    def inputs_timestamp(self):
        return max([self.timestamp(inp) for inp in self.inputs.values()])
            
    @property
    def outputs_timestamp(self):
        return min([self.timestamp(inp) for inp in self.outputs.values()])



    @property
    def outputs(self):
        return dict(map(lambda x: (x,local_dir + '/' + self.name + '/' +x),self.outputs_files))

    @property
    def inputs(self):
        return dict(map(lambda x: (x,local_dir + '/' + self.name + '/' +x),self.inputs_files))


class PigProcess(Process):
    
    def clear(self):
        for output in self.outputs:
            if 0 != call(['hadoop','fs','-rmr',output]):
                warn("can't remove %s from hdfs"%output)

    def run(self):
            global cache_hdfs_timestamp
            inputs_map = map(lambda (x,y): '%s=%s'%(x,y),self.inputs.items())
            inputs = reduce(lambda x,y :  x + ['-p',y], inputs_map,[])
            outputs_map = map(lambda (x,y): '%s=%s'%(x,y),self.outputs.items())
            outputs = reduce(lambda x,y :  x + ['-p',y], outputs_map,[])
            info('launch `%s`'%' '.join(['pig','-F','-p','name=%s'%self.name ] + outputs + inputs + [self.script]))
            retcode = call(['pig','-F','-p','name=%s'%self.name ] + outputs + inputs + [self.script])
            info('remove timestamps from cache')
            cache_hdfs_timestamp = {}

            if retcode != 0 :
                error("script %s don't finish with status 0"%self.script)
                return False

            return True

    
    @property
    def inputs_timestamp(self):
        return max([pigTimeStamp(inp) for inp in self.inputs.values()])
            
    @property
    def outputs_timestamp(self):
        return min([pigTimeStamp(inp) for inp in self.outputs.values()])

    @property
    def outputs(self):
        return dict(map(lambda x : (x,hdfs_dir + self.name +'/' + x),self.outputs_files))

    @property
    def inputs(self):
        return dict(map(lambda x : (x,hdfs_dir + self.name +'/' + x),self.inputs_files))

       
class FromLocalProcess(Process):
    @property
    def outputs(self):
        return dict(map(lambda x : (x,hdfs_dir + self.name +'/' + x),self.outputs_files))

    @property
    def outputs_timestamp(self):
        return min(pigTimeStamp(o) for o in self.outputs.values())
 

class ToLocalProcess(Process):
    @property
    def inputs(self):
        return dict(map(lambda x : (x,hdfs_dir + self.name +'/' + x),self.inputs_files))
    

    @property
    def inputs_timestamp(self):
        return min(pigTimeStamp(o) for o in self.inputs.values())


class WhilePigProcess(PigProcess):


    loop_files = []

    @property
    def inputs(self):
        result = super(WhilePigProcess,self).inputs
        if self._iteration > 0:
            for lf in self.loop_files:
                result[lf+'_in'] = lf + '_' + str(self._iteration -1)

        return result
    
    @property
    def outputs(self):
        result = super(WhilePigProcess,self).outputs
        if self._iteration >= 0:
            for lf in self.loop_files:
                result[lf+'_out'] = lf + '_' + str(self._iteration )

        return result

    def __init__(self,*args,**kwargs):
        super(WhilePigProcess,self).__init__(*args,**kwargs)
        self._iteration = -1

    def conditional(self):
        return False

    def post_loop(self):
        for lf in loop_files:
            call(['hadoop','fs','-rmr',
                hdfs_dir + self.name + '/' + lf + '_out'])

            call(['hadoop','fs','-mv',
                hdfs_dir + self.name + '/' + lf + str(self._iteration),
                hdfs_dir + self.name + '/' + lf + '_out'])

    def run(self):
        self._iteration = 0
        while self.conditional():
            super(WhilePigProcess,self).run()
            self._iteration += 1

        self.post_loop()
        self._iteration = -1

class ShipPigProcess(PigProcess):
    ship_files = []

    @property
    def inputs(self):
        result =  dict(map(lambda x : (x,hdfs_dir + self.name +'/' + x),self.inputs_files))
        result.update(dict(map(lambda x: (x,local_dir + '/' + self.name + '/' +x),self.ship_files)))
        return result 

    @property
    def inputs_timestamp(self):
        hdfs_files =  map(lambda x : hdfs_dir + self.name +'/' + x,self.inputs_files)
        hdfs_min = min(pigTimeStamp(o) for o in hdfs_files)
        local_files = map(lambda x: local_dir + '/' + self.name + '/' +x,self.ship_files)

        local_min = min(self.timestamp(o) for o in local_files)
        return min(hdfs_min,local_min)


class SourceProcess(Process):

    def start(self,dry=False):
        """
        Empty run, this process is You !
        """
        return True

    @property
    def script_timestamp(self):
        return self.outputs_timestamp

    uptodate = True


