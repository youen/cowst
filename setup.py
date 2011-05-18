#!/usr/bin/env python

from setuptools import setup,find_packages

import djity_cowst

setup(name=djity_cowst.pip_name,
    version=djity_cowst.version,
    description=djity_cowst.description,
    author=djity_cowst.author,
    author_email=djity_cowst.author_email,
    url=djity_cowst.url,
    packages=['djity_cowst'],
    package_data={'djity_cowst':[
		'templates/djity_cowst/*.html',
		'media/*/*']
		},
)
