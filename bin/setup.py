#!/usr/bin/python3
# coding: utf-8
# packages
import warnings
warnings.filterwarnings("ignore")
from writeConfig import WriteConfig
import os, sys, stat
import maveric.projectDetails as proDet
import maveric.projectConfig as cfg
import maveric.glogging as glg
import createConfigAndlog 
projectHome=proDet.getProjectHome(__file__)
projectIDArr=proDet.getProjectName(projectHome)

projectConfig=createConfigAndlog.getLogAndConfig(projectHome,projectIDArr)[0]
log=createConfigAndlog.getLogAndConfig(projectHome,projectIDArr)[1]

from packageinstallation import PackageInstallation

packInsClass=PackageInstallation(projectConfig,log)
pkginstall=packInsClass.main()

import sys
from dataGenv3 import DataGenerator
#import testDataGenerated


def applicationInfo():
	writeConf=input ("Do you want to change config details(Customer, Account) in applicationInfo ?:")
	appInf=writeConf.upper()
	if appInf == 'YES' or appInf == 'Y':
		appInfoClass=WriteConfig(projectConfig,log)
		appInfo=appInfoClass.changeinputs()

	else:		
		print("Continuing with existing config details")
		log.info("Continuing with existing config details")

def main():
	appinf= applicationInfo()
	dataGen= DataGenerator(projectConfig,log).main()
	#testDataGenerated.unittest.main()
if __name__ == '__main__':
	main()

