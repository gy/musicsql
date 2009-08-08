#!/usr/bin/env python

import sys
import os.path
from distutils.core import setup


if sys.platform == 'darwin' or sys.platform.startswith('linux'):
    path = "/usr/bin"
elif sys.platform.startswith('win') or sys.platform.startswith('mingw'):
    path = os.path.dirname(sys.executable)
else:
    sys.exit("The %s platform is not supported yet." % sys.platform)

setup(name='MusicSQL',
      version='0.1.1',
      description='MusicSQL libraries for musicology database queries',
      author='Bret Aarden',
      author_email='bret.aarden@gmail.com',
      url='http://www.musicsql.org',
      packages=['musicsql', 'musicsql.alchemy', 'musicsql.database'],
      package_data={'musicsql': ['CONFIG', 'dtd/*']},
      data_files=[(path, ['scripts/musicsqlcmd.py'])]
      )
