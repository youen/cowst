"""
Models for Djity application cowst
"""

from django.db import models

from djity.project.models import Module
from djity.utils.inherit import SuperManager
from djity.utils import djreverse
from djity.utils.security import sanitize
from djity.portlet.models import TemplatePortlet, TextPortlet
from djity.transmeta import TransMeta

class Cowst(Module):
    """
    Main class of this new Djity application. This class acts like a proxy between
    your application and Djity, allowing Djity to manage its modules in a
    standard way.

    At creation, this application simply displays a message in a tab.
    """
    #get the module manager
    objects = SuperManager()

    #use transmeta for international message
    #__metaclass__ = TransMeta
    

    #class Meta:
        #specify that the field 'message' is translatable
        #translate = ('message',)
        

    def save(self,*args,**kwargs):
        """
        Redefine save() from Module in order to attach portlets at creation.
        """
        # Is the module new ?
        new = (self.id == None)

        super(Cowst,self).save(*args, **kwargs)

        #if new:
            #A standard text portlet, its content will be automatically editable
            #TextPortlet(content=sanitize("This is a tab level text portlet edit me !"), container=self,position="left", rel_position=0).save()

            # A standard template portlet, it will simply render a template
            #TemplatePortlet(template="djity_cowst/cowst_portlet.html",container=self,position="right",rel_position=0).save()

            # A custom template defined in models.py of the application
            #CowstPortlet(container=self,position="right",rel_position=0).save()
    

    def djity_url(self,context):
        """
        return the application's start page
        """
        return djreverse('cowst-main',context)

class YagoClass(models.Model):
    """
    A Yago Class
    """
    #dbpedia uri of the YAGO class
    md5 = models.BigIntegerField( db_index=True, primary_key=True)
    label = models.CharField(max_length=200,blank=True)
    parents = models.ManyToManyField('self', related_name='children')
    
    def __unicode__(self):
        try:
            return Dico.objects.get(code=self.md5).uri[1:-1]
        except Dico.DoesNotExist:
            return unicode(self.md5)


class Dico(models.Model):
    """
    A Dico for md5 decoding
    """
    code = models.BigIntegerField(primary_key=True, db_index=True)
    uri = models.CharField(max_length=255,  db_index=True)

    def __unicode__(self):
        return '%s -> %s'%(self.code, self.uri)
    
    class Meta:
        unique_together = ('code','uri')

class Template(models.Model):
    """
    A Wikipedia template
    """
    #the dbpedia uri of the template
    md5 = models.BigIntegerField(primary_key=True, db_index=True)
    label = models.CharField(max_length=200,blank=True)
    used_with = models.ManyToManyField('YagoClass', through='TemplateHistogram', related_name='used_by_template')
    alpha = models.ForeignKey('YagoClass',blank=True,null=True, related_name='alpha_class_of_template')

    def __unicode__(self):
        try:
            return Dico.objects.get(code=self.md5).uri[1:-1]
        except Dico.DoesNotExist:
            return unicode(self.md5)

    @property
    def size(self):
        return TemplateHistogram.objects.get(template =self, yagoClass = self.alpha).count


class TemplateHistogram(models.Model):
    """
    Howmany ressources of a Yago class used a specific template
    """
    template = models.ForeignKey('Template')
    yago_class = models.ForeignKey('YagoClass')
    count = models.PositiveIntegerField()


    class Meta:
        unique_together = ('template','yago_class')

class InfoboxProperty(models.Model):
    """
    A raw property of DBpedia
    """

    md5 = models.BigIntegerField(primary_key=True, db_index=True)
    label = models.CharField(max_length=200,blank=True)
    used_with = models.ManyToManyField('YagoClass', through='InfoboxPropertyHistogram', related_name='used_by_properties')
    alpha = models.ForeignKey('YagoClass',blank=True,null=True, related_name='alpha_class_of_properties')

    def __unicode__(self):
        try:
            return Dico.objects.get(code=self.md5).uri[1:-1]
        except Dico.DoesNotExist:
            return unicode(self.md5)

    @property
    def size(self):
        return InfoboxPropertyHistogram.objects.get(infobox_property =self, yago_class = self.alpha).count

class InfoboxPropertyHistogram(models.Model):
    """
    How many ressources of a Yago class used a specific infobox property
    """
    infobox_property = models.ForeignKey('InfoboxProperty')
    yago_class = models.ForeignKey('YagoClass')
    count = models.PositiveIntegerField()
    sample = models.ManyToManyField('Instance')

    class Meta:
        unique_together = ('infobox_property','yago_class')


class CowstPortlet(TemplatePortlet):
    def save(self,*args,**kwargs):
        # Is the portlet new ?
        new = (self.id == None)

        super(CowstPortlet,self).save(*args, **kwargs)
        
        if new:    
            self.onload = "$('.cowst-load-portlet').cowst_widget();" 
            self.template = "djity_cowst/cowst_portlet2.html"
            super(CowstPortlet,self).save(*args, **kwargs)

class Instance(models.Model):
    """
    A DBpedia Instance
    """
    #dbpedia uri of the YAGO class
    md5 = models.BigIntegerField( db_index=True, primary_key=True)
    label = models.CharField(max_length=200,blank=True)
    rank = models.FloatField()
    
    def __unicode__(self):
        try:
            return Dico.objects.get(code=self.md5).uri[1:-1]
        except Dico.DoesNotExist:
            return unicode(self.md5)


