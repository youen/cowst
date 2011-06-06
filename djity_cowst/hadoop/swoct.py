# -*- encoding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
from pigprocess import *


class MD5EncodingMappingProperties(PigProcess):
    dependencesProcess = []
    script = 'pig/parsen3mappingproperties.pig'
    outputs_files = ['mappingbased_properties_md5', 'mappingbased_properties_list']
    inputs_files = ['mappingbased_properties_nt']

class MD5EncodingInstanceTypes(PigProcess):
    dependencesProcess = []
    script = 'pig/parsen3instance_types.pig'
    outputs_files = ['instance_types_md5']
    inputs_files = ['instance_types_nt']

class MD5EncodingYagoDbpedia(PigProcess):
    dependencesProcess = []
    script = 'pig/parsen3yagodbpedia.pig'
    outputs_files = ['yago_dbpedia','yago_dbpedia_md5']
    inputs_files = ['dbpedia_yago_matching_nt']


class MD5EncodingYago(PigProcess):
    dependencesProcess = []
    script = 'pig/parsen3yago.pig'
    outputs_files = ['yago_subclassof_md5','yago_subclassof_tsv','yago_types_md5','yago_size']
    inputs_files = ['yago_links_nt']

class MD5EncodingInfoboxProperties(PigProcess):
    dependencesProcess = []
    script = 'pig/parsen3infoboxproperties.pig'
    outputs_files = ['infobox_properties_md5', 'infobox_properties_list']
    inputs_files = ['infobox_properties_nt']


class MD5EncodingTemplates(PigProcess):
    dependencesProcess = []
    script = 'pig/parsen3templates.pig'
    outputs_files = ['templates_md5']
    inputs_files = ['templates_nt']

class MD5EncodingSubClassOf(PigProcess):
    dependencesProcess = []
    script = 'pig/parsen3subclassof.pig'
    outputs_files = ['subclassof_md5','pig_subclassof_md5']
    inputs_files = ['subclassof_nt']


class YagoSelect(PigProcess):
    dependencesProcess = [MD5EncodingYago]
    script= 'pig/yago_select.pig'
    inputs_files = ['yago_size']
    outputs_files = ['yago_selected']


class InferenceProcess(PigProcess):
    dependencesProcess = [MD5EncodingYago, YagoSelect]
    script = 'pig/infer.pig'
    inputs_files = ['yago_types_md5','yago_subclassof_md5','yago_selected']
    outputs_files = ['instance_infered','yago_classes_list']

class MD5EncodingPageLinks(PigProcess):
    dependencesProcess = []
    script = 'pig/parsen3page_links.pig'
    inputs_files = ['page_links_nt']
    outputs_files = ['page_links']


class PrePageRank(PigProcess):
    dependencesProcess = [MD5EncodingPageLinks]
    script = 'pig/pre_pagerank.pig'
    inputs_files = ['page_links']
    outputs_files = ['page_contribs','zero_contribs','pagerank_in']

class PageRank(WhilePigProcess):


    loop_files = ['pagerank','pagerank_avg']
    
    def conditional(self):
        print self.inputs
        return True

    dependencesProcess = [PrePageRank]
    script = 'pig/pagerank.pig'
    inputs_files = ['page_links','page_contribs','zero_contribs','pagerank_in']
    outputs_files = ['pagerank_out','pagerank_avg_out']

class TemplateHistogram(PigProcess):
    dependencesProcess = [MD5EncodingTemplates,InferenceProcess]
    script = 'pig/template_histogram.pig'
    outputs_files = ['template_histogram']
    inputs_files = ['templates_md5','yago_infered']

class InfoboxPropertiesHistogram(PigProcess):
    dependencesProcess = [MD5EncodingInfoboxProperties,InferenceProcess,PageRank]
    script = 'pig/infobox_properties_histogram.pig'
    inputs_files = ['infobox_properties_md5','instance_infered','pagerank_out']
    outputs_files = ['infobox_properties_histogram']

class InfoboxPropertiesLocalHistogram(PigProcess):
    dependencesProcess = [MD5EncodingInfoboxProperties,InferenceProcess,PageRank]
    script = 'pig/infobox_properties_local_histogram.pig'
    inputs_files = ['infobox_properties_md5','instance_infered','pagerank_out']
    outputs_files = ['infobox_properties_local_histogram']


"""
class SwitchPageRank1(PigProcess):
    dependencesProcess = [PageRank1]
    script = 'pig/switchpagerank.pig'
    inputs_files = ['pagerank_out']
    outputs_files = ['pagerank_in']


class PageRank2(PigProcess):
    dependencesProcess = [SwitchPageRank1]
    script = 'pig/pagerank.pig'
    inputs_files = ['page_links','page_contribs','zero_contribs','pagerank_in']
    outputs_files = ['pagerank_out']
"""



class Instances(PigProcess):
    dependencesProcess = [InfoboxPropertiesHistogram,InfoboxPropertiesLocalHistogram,PageRank]
    script = 'pig/instances.pig'
    inputs_files = ['infobox_properties_local_histogram','infobox_properties_histogram','pagerank_out']
    outputs_files = ['instances_tsv']

class Dico(PigProcess):
    dependencesProcess = [Instances]
    script = 'pig/dico.pig'
    inputs_files = ['subclassof_nt','templates_nt',  'infobox_properties_nt','instances_tsv']
    outputs_files = ['dico']



if __name__ == '__main__' :
    from logging import basicConfig,INFO,DEBUG

    basicConfig(level=DEBUG,format="%(asctime)s - %(levelname)s - %(message)s")


    from sys import argv
    dry = False
    if len(argv) > 2:
        dry = True
    job = Dico(argv[1])
    #job = AlphaProcess(argv[1])
    job.start(dry=dry)

