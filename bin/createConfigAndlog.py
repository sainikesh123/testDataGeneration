import os
import maveric.projectDetails as proDet
import maveric.projectConfig as cfg
import maveric.glogging as glg
import sys

def getLogAndConfig(projectHome,projectIDArr):
    if (projectIDArr[0]):
        projectID = projectIDArr[1]
    else:
        print("Cannot read the config file")
        print(projectIDArr[1])
        print("For this to work, create a '.maveric' file in '"+projectHome+"' with a line gnthmPID=somevalue")
        sys.exit()

    readProjectConfig = cfg.readConfig(projectHome)
    if (readProjectConfig[0]):
        projectConfig = readProjectConfig[1]
        logger= glg.getLogger(projectID,projectConfig['logging']['logDir'],int(projectConfig['logging']['logLevel']))
    else:
        print("Cannot read the config file")
        print(readProjectConfig[1])
        print("For this to work, create the 'application.ini' file in '"+projectHome+"/etc/' with values of logDir=/somedir and logLevel=10")
        sys.exit()
    return projectConfig,logger
