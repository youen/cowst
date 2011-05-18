from django.conf.urls.defaults import *

prefix = 'cowst'

urlpatterns = patterns('djity_cowst.views',
    url(r'^$','cowst_view',{'module_name':'cowst'},name='cowst-main'),
)
