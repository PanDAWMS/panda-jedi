#
#
# Setup prog for JEDI
#
#
# set PYTHONPATH to use the current directory first
import sys
sys.path.insert(0,'.')  # noqa: E402

import os
import re
import sys
import socket
import getpass
import grp
import site

import PandaPkgInfo
from setuptools import setup
from setuptools.command.install import install as install_org
import distutils
from distutils.command.install_data import install_data as install_data_org

# define user name and group
panda_user = 'atlpan'
panda_group = 'zp'

# get release version
release_version = PandaPkgInfo.release_version
if 'BUILD_NUMBER' in os.environ:
    release_version = '{0}.{1}'.format(release_version,os.environ['BUILD_NUMBER'])

# get panda specific params
optPanda = {}
newArgv  = []
idx = 0
while idx < len(sys.argv):
    tmpArg = sys.argv[idx]
    if tmpArg.startswith('--panda_'):
        # panda params
        idx += 1
        if len(tmpArg.split('=')) == 2:
            # split to par and val if = is contained
            tmpVal = tmpArg.split('=')[-1]
            tmpArg = tmpArg.split('=')[0]
        elif len(tmpArg.split('=')) == 1:
            tmpVal = sys.argv[idx]
            idx += 1
        else:
            raise RuntimeError("invalid panda option : %s" % tmpArg)
        # get key
        tmpKey = re.sub('--panda_','',tmpArg)
        # set params
        optPanda[tmpKey] = tmpVal
    else:
        # normal opts
        idx += 1
        newArgv.append(tmpArg)
# set new argv
sys.argv = newArgv


# set overall prefix for bdist_rpm
class install_panda(install_org):
    def initialize_options (self):
        install_org.initialize_options(self)

    # disable egg
    def finalize_options(self):
        install_org.finalize_options(self)
        self.single_version_externally_managed = True


# generates files using templates and install them
class install_data_panda (install_data_org):

    def initialize_options (self):
        install_data_org.initialize_options (self)
        self.install_purelib = None
        self.panda_user = panda_user
        self.panda_group = panda_group
        self.virtual_env = ''
        self.virtual_env_setup = ''
        if 'VIRTUAL_ENV' in os.environ:
            self.virtual_env = os.environ['VIRTUAL_ENV']
            self.virtual_env_setup = 'source {0}/bin/activate'.format(os.environ['VIRTUAL_ENV'])
        elif sys.executable:
            venv_dir = os.path.dirname(os.path.dirname(sys.executable))
            py_venv_activate = os.path.join(venv_dir, 'bin/activate')
            if os.path.exists(py_venv_activate):
                self.virtual_env = venv_dir
                self.virtual_env_setup = 'source {0}'.format(py_venv_activate)

    def finalize_options (self):
        # set install_purelib
        self.set_undefined_options('install',
                                   ('install_purelib','install_purelib'))
        # set prefix for pip
        if not hasattr(self, 'prefix'):
            self.prefix = site.PREFIXES[0]
        # set reaming params
        install_data_org.finalize_options(self)
        # set hostname
        if 'hostname' in optPanda and optPanda['hostname'] != '':
            self.hostname = optPanda['hostname']
        else:
            self.hostname = socket.gethostname()
        # set user and group
        if 'username' in optPanda and optPanda['username'] != '':
            self.username = optPanda['username']
        else:
            self.username = getpass.getuser()
        if 'usergroup' in optPanda and optPanda['usergroup'] != '':
            self.usergroup = optPanda['usergroup']
        else:
            self.usergroup = grp.getgrnam(self.username).gr_name

    def run (self):
        # setup.py install sets install_dir to /usr
        if self.install_dir == '/usr':
            self.install_dir = '/'
        elif 'bdist_wheel' in self.distribution.get_cmdline_options():
            # wheel
            self.install_dir = self.prefix
            self.install_purelib = distutils.sysconfig.get_python_lib()
            self.install_scripts = os.path.join(self.prefix, 'bin')
        if not self.install_dir:
            if self.root:
                # rpm
                self.install_dir = self.prefix
                self.install_purelib = distutils.sysconfig.get_python_lib()
                self.install_scripts = os.path.join(self.prefix, 'bin')
            else:
                # sdist
                if not self.prefix:
                    if '--user' in self.distribution.script_args:
                        self.install_dir = site.USER_BASE
                    else:
                        self.install_dir = site.PREFIXES[0]
                else:
                    self.install_dir = self.prefix
        # remove /usr for bdist/bdist_rpm
        match = re.search('(build/[^/]+/dumb)/usr',self.install_dir)
        if match is not None:
            self.install_dir = re.sub(match.group(0),match.group(1),self.install_dir)
        # remove /var/tmp/*-buildroot for bdist_rpm
        match = re.search('(/var/tmp/.*-buildroot)/usr',self.install_dir)
        if match is not None:
            self.install_dir = re.sub(match.group(0),match.group(1),self.install_dir)
        # create tmp area
        tmpDir = 'build/tmp'
        self.mkpath(tmpDir)
        new_data_files = []
        for destDir,dataFiles in self.data_files:
            newFilesList = []
            for srcFile in dataFiles:
                # check extension
                if not srcFile.endswith('.template'):
                    raise RuntimeError("%s doesn't have the .template extension" % srcFile)
                # dest filename
                destFile = re.sub('(\.exe)*\.template$','',srcFile)
                destFile = re.sub(r'^templates/', '', destFile)
                destFile = '%s/%s' % (tmpDir,destFile)
                # open src
                inFile = open(srcFile)
                # read
                filedata=inFile.read()
                # close
                inFile.close()
                # replace patterns
                for item in re.findall('@@([^@]+)@@',filedata):
                    if not hasattr(self,item):
                        raise RuntimeError('unknown pattern %s in %s' % (item,srcFile))
                    # get pattern
                    patt = getattr(self,item)
                    # remove install root, if any
                    if self.root is not None and patt.startswith(self.root):
                        patt = patt[len(self.root):]
                    # remove build/*/dump for bdist
                    patt = re.sub('build/[^/]+/dumb','',patt)
                    # remove /var/tmp/*-buildroot for bdist_rpm
                    patt = re.sub('/var/tmp/.*-buildroot','',patt)
                    # replace
                    filedata = filedata.replace('@@%s@@' % item, patt)
                # write to dest
                if '/' in destFile:
                    destSubDir = os.path.dirname(destFile)
                    if not os.path.exists(destSubDir):
                        os.makedirs(destSubDir)
                oFile = open(destFile,'w')
                oFile.write(filedata)
                oFile.close()
                # chmod for exe
                if srcFile.endswith('.exe.template'):
                    os.chmod(destFile, 0o755)
                # append
                newFilesList.append(destFile)
            # replace dataFiles to install generated file
            new_data_files.append((destDir,newFilesList))
        # install
        self.data_files = new_data_files
        install_data_org.run(self)


# setup for distutils
setup(
    name="panda-jedi",
    version=release_version,
    description='JEDI Package',
    long_description='''This package contains JEDI components''',
    license='GPL',
    author='Panda Team',
    author_email='atlas-adc-panda@cern.ch',
    url='https://twiki.cern.ch/twiki/bin/view/Atlas/PanDA',
    packages=[ 'pandajedi',
               'pandajedi.jedicore',
               'pandajedi.jediexec',
               'pandajedi.jeditest',
               'pandajedi.jedidog',
               'pandajedi.jediddm',
               'pandajedi.jedigen',
               'pandajedi.jedisetup',
               'pandajedi.jediorder',
               'pandajedi.jediconfig',
               'pandajedi.jedirefine',
               'pandajedi.jedithrottle',
               'pandajedi.jedibrokerage',
               'pandajedi.jedipprocess',
               'pandajedi.jedimsgprocessor',
              ],
    install_requires=[
        'six',
        'panda-common>=0.0.33',
        'panda-server>=0.0.37',
        'python-daemon',
        'numpy',
        'pyyaml',
        'requests',
        'packaging'
    ],
    extras_require={
        'oracle': ['cx_Oracle'],
        'mysql': ['mysqlclient'],
        'postgres': ['psycopg2-binary'],
        'rucio': ['rucio-clients'],
        'atlasprod': ['cx_Oracle', 'rucio-clients', 'idds-common', 'idds-client'],
    },
    data_files=[
                # config and cron files
                ('etc/panda', ['templates/panda_jedi.cfg.rpmnew.template',
                                'templates/panda_jedi.cron.template',
                                'templates/logrotate.d/panda_jedi.template',
                               ]
                 ),
                # sysconfig
                ('etc/sysconfig', ['templates/sysconfig/panda_jedi.template',
                                   ]
                 ),
                # init script
                ('etc/rc.d/init.d', ['templates/init.d/panda_jedi.exe.template',
                                   ]
                 ),
                # exec
                ('usr/bin', ['templates/panda_jedi-reniceJEDI.exe.template',
                             ]
                 ),
                ],
    cmdclass={'install': install_panda,
              'install_data': install_data_panda}
)
