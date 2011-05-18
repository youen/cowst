# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
import gzip

from django.core.management.base import BaseCommand, CommandError

from djity_cowst.models import YagoClass
import djity_cowst
default_path = djity_cowst.__path__[0] + '/data/subclassof.nt.gz'

class Command(BaseCommand):

    def handle(self,*args,**options):
        yago_file = raw_input('The yago subClassOf deffinition from dbpedia nt.gz format (default %s):'%default_path)
        if yago_file == "":
            yago_file = default_path

        f = gzip.open(yago_file)
        for line in f:
            child , test, parent = line.split(' ',3)[:3]
            child = child[1:-1]
            parent = parent[1:-1]
            print child,parent
            try:
                childClass = YagoClass.objects.get(uri=child)
            except YagoClass.DoesNotExist:
                childClass = YagoClass(uri=child)

            parentClass =YagoClass.objects.get_or_create(uri=parent)

            childClass.parent = parentClass
            childClass.save()




