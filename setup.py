from setuptools import setup

setup(name='ostckutil',
      version='0.6',
      description='Open Stack utils, mainly around swift access. Key utility is an rsync for Swift.',
      url='https://github.com/proneel/ostckutil',
      author='proneel',
      author_email='proneel@gmail.com',
      license='MIT',
      packages=['ostckutil'],
      scripts=['bin/swiftsync', 'bin/swiftbulkdel', 'bin/ostcklogusage'],
      install_requires=[ 'python-novaclient', 'python-cinderclient', 'python-swiftclient' ],
      zip_safe=False)
