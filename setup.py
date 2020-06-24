from setuptools import setup


setup(name='sda_orchestrator',
      version='0.0.1',
      url='',
      project_urls={
          'Source': '',
      },
      license='Apache 2.0',
      author='CSC Developers',
      author_email='',
      description='something',
      long_description="",
      packages=['sda_orchestrator', 'sda_orchestrator/utils', 'sda_orchestrator/templates'],
      # If any package contains *.json, include them:
      package_data={'': ['*.html']},
      entry_points={
          'console_scripts': [
              'sdainbox=sda_orchestrator.inbox_consume:main',
              'sdaverified=sda_orchestrator.verified_consume:main',
              'sdacomplete=sda_orchestrator.complete_consume:main',
              'webapp=sda_orchestrator.app:main',
          ]
      },
      platforms='any',
      classifiers=[  # Optional
          # How mature is this project? Common values are
          #   3 - Alpha
          #   4 - Beta
          #   5 - Production/Stable
          'Development Status :: 3 - Alpha',

          # Indicate who your project is intended for
          'Intended Audience :: Developers',
          'Intended Audience :: Information Technology',

          # Pick your license as you wish
          'License :: OSI Approved :: Apache Software License',

          'Programming Language :: Python :: 3.7',
      ],
      install_requires=['asyncpg', 'psycopg2', 'amqpstorm',
                        'aiohttp-jinja2', 'jinja2', 'aiohttp']
      )
