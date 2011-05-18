from django.shortcuts import render_to_response
from django.contrib import messages
from django.utils.translation import ugettext_lazy as _

from djity.utils.decorators import djity_view
from djity.transmeta import get_lang_version

@djity_view()
def cowst_view(request,context=None):
    """
    Main view of a bare module after 'djity-admin.py create_module' was called
    """
    # Get the current instance of the module to render
    cowst = context['module']
    context['djity_cowst_lang'] =  get_lang_version(cowst,'message')
    # fetch its message
    message = _(cowst.message)
    # will display a notification
    messages.add_message(request,messages.INFO,unicode(message))
    # update djity's context for templates to render correctly
    context.update({'view':'cowst','message':message})
    # render the template and return it
    return render_to_response('djity_cowst/cowst.html',context)
