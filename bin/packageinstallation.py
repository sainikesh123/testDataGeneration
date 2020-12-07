#!/usr/bin/env python3
# coding: utf-8
# packages
import warnings
warnings.filterwarnings("ignore")
import subprocess
import sys
import os
import traceback
from pkg_resources import WorkingSet , DistributionNotFound
from setuptools.command.easy_install import main as install

class PackageInstallation:
    def __init__(self,projectConfig,myLogger):
        self.projectConfig=projectConfig
        self.myLogger=myLogger
        
    def sys_installed_packages(self):
        try:
            reqs = subprocess.check_output([sys.executable, '-m', 'pip', 'freeze']) 
        except Exception as e:
            self.myLogger.error("Please ensure that pip or pip3 is installed on your system and redo the setup - Alert message is"% (i))
        installed_packages = [r.decode().split('==')[0] for r in reqs.split()]
        return installed_packages

    def sys_install_packages(self,installed_packages,requirements):
        packages=[]
        with open(requirements, "rt") as f:
            for line in f:
                l = line.strip()
                package = l.split(',')
                package=package[0]
                packages.append(package)

        for i in packages:
            if i in installed_packages:
                continue
                self.myLogger.info("The %s package is already installed" % (i))
            if i not in installed_packages:
                working_set = WorkingSet()
                try:
                    dep = working_set.require('paramiko>=1.0')
                except DistributionNotFound:
                    pass

                whoami=os.getlogin()
                if whoami =='root':
                    installPackage=install([i])
                    self.myLogger.info("Newlly installation of %s is sucessfully done"% (installPackage))
                if whoami !='root':
                    try:
                        installPackage=subprocess.check_call(["pip", "install","--user", i])
                        #nltkpkg=subprocess.call(['python','-m','nltk.downloader','all'])
                        self.myLogger.info("Newlly installation of %s is sucessfully done"% (installPackage))
                    except:
                        try:
                            installPackage=subprocess.check_call(["pip3", "install","--user", i])
                            #nltkpkg=subprocess.call(['python3','-m','nltk.downloader','all'])
                            self.myLogger.info("Newlly installation of %s is sucessfully done"% (installPackage))
                        except Exception as e:
                            e = sys.exc_info()
                            self.myLogger.error("the above error occured while installing %s package"% (e))

    def main(self):
        requirements =self.projectConfig.get('default','requirements')
        installed_packages= self.sys_installed_packages()
        install_packages=self.sys_install_packages(installed_packages,requirements)