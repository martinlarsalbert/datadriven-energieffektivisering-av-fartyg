from setuptools import find_packages, setup
import os.path
import subprocess
import sys
from setuptools.command.install import install as _install


def get_pwd_candidates():
    """attempt to get a working directory for running `pre-commit install` in.
    - pip installs in a temporary directory so `os.getcwd()` won't work
    - `$PWD` is likely to work, but won't work on windows
    - `$VIRTUAL_ENV` probably works, if your virtualenv is in your git repo
    - `os.path.dirname(sys.executable)` is another potential option
    """
    for v in ('PWD', 'VIRTUAL_ENV'):
        if os.environ.get(v):
            yield os.environ[v]
    yield os.path.dirname(sys.executable)


class install(_install):
    def run(self):
        _install.run(self)
        for pwd in get_pwd_candidates():
            cmd = (sys.executable, '-m', 'pre_commit', 'install')
            if not subprocess.call(cmd, cwd=pwd):
                return
        else:
            raise SystemExit(r'wellp, I tried ¯\__(ツ)__/¯')
           
        subprocess.run(['jupyter', 'labextension', 'install', 'jupyterlab_templates'])
        subprocess.run(['jupyter', 'serverextension', 'enable', '--py', 'jupyterlab_templates'])
        
           
        


try:
    from wheel.bdist_wheel import bdist_wheel as _bdist_wheel

    class bdist_wheel(_bdist_wheel):
        def run(self):
            raise SystemExit('this is fine')
except ImportError:
    bdist_wheel = None

setup(
    name='src',
    packages=find_packages(),
    version='0.1.0',
    description='Some ML stuff for the project datadriven-energieffektivisering-av-fartyg',
    author='Martin Alexandersson',
    license='MIT',
    install_requires=['pre-commit'],
    cmdclass={'bdist_wheel': bdist_wheel, 'install': install},
)
