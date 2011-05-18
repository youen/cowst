from django.shortcuts import render_to_response
from django.contrib import messages
from django.utils.translation import ugettext_lazy as _

from djity.utils.decorators import djity_view
from djity.transmeta import get_lang_version

from djity_cowst.models import Template

@djity_view()
def cowst_view(request,context=None):
    """
    Main view of a bare module after 'djity-admin.py create_module' was called
    """
    # Get the current instance of the module to render
    cowst = context['module']

    # update djity's context for templates to render correctly
    context.update({'view':'cowst'})
    templates = Template.objects.all()
    if len(templates) == 0:
        return render_to_response('djity_cowst/cowst_start.html',context)
    # render the template and return it
    return render_to_response('djity_cowst/cowst.html',context)
