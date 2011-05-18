"""
Ajax views for cowst.
"""

from django.utils.translation import ugettext_lazy as _
from django.utils.translation import activate, get_language
from djity.utils.decorators import djity_view
from djity.utils.security import sanitize
from djity.transmeta import get_value,set_value

from django.conf import settings

from dajaxice.core import dajaxice_functions
register = lambda name:dajaxice_functions.register_function('djity_cowst.ajax',name)

# permission needed to access a view can be 'view', 'edit', 'manage'
@djity_view(perm='manage')
def load_data(request,js_target,context=None):
    js_target.message(_('Data loaded!'))
    js_target.reload()

#register your view in dajaxice
register('load_data')

