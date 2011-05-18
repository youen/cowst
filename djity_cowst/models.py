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
    uri = models.CharField(max_length=200, primary_key=True, db_index=True)
    label = models.CharField(max_length=200,blank=True)
    parentClass = models.ForeignKey('YagoClass')

class Template(models.Model):
    """
    A Wikipedia template
    """
    #the dbpedia uri of the template
    uri = models.CharField(max_length=200, primary_key=True, db_index=True)
    label = models.CharField(max_length=200,blank=True)
    used_with = models.ManyToManyField('YagoClass', through='TemplateHistogram')



class TemplateHistogram(models.Model):
    """
    Howmany ressources of a Yago class used a specific template
    """
    template = models.ForeignKey('Template')
    yagoClass = models.ForeignKey('YagoClass')
    count = models.PositiveIntegerField()


class CowstPortlet(TemplatePortlet):
    def save(self,*args,**kwargs):
        # Is the portlet new ?
        new = (self.id == None)

        super(CowstPortlet,self).save(*args, **kwargs)
        
        if new:    
            self.onload = "$('.cowst-load-portlet').cowst_widget();" 
            self.template = "djity_cowst/cowst_portlet2.html"
            super(CowstPortlet,self).save(*args, **kwargs)

