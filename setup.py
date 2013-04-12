from setuptools import setup

setup(name='utils4scrapy',
      version='0.2',
      author='MOON_CLJ',
      install_requires=[
          'simplejson',
          'pymongo',
          'urllib3',
          'raven',
          'redis',
          'scrapy'
      ],
      packages=['utils4scrapy'],
      )
