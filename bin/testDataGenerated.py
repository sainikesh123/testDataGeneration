import warnings
warnings.filterwarnings("ignore")
import unittest
import sys
import os
from dataGenerations import DataGenerator
from owlready2 import *
from collections import defaultdict
import string
import ast 
import pandas as pd
import rdflib
import random
import createConfigAndlog 
from dataGenerations import DataGenerator
import os, sys, stat
import maveric.projectDetails as proDet
import maveric.projectConfig as cfg
import maveric.glogging as glg
from packageinstallation import PackageInstallation
import sys
import createConfigAndlog 
from dataGenerations import DataGenerator
projectHome=proDet.getProjectHome(__file__)
projectIDArr=proDet.getProjectName(projectHome)
projectHome=proDet.getProjectHome(__file__)
projectIDArr=proDet.getProjectName(projectHome)

projectConfig=createConfigAndlog.getLogAndConfig(projectHome,projectIDArr)[0]
log=createConfigAndlog.getLogAndConfig(projectHome,projectIDArr)[1]

datagen= DataGenerator(projectConfig,log)
data_prop=datagen.extractDataProperties()
data_prop_datatypes=datagen.extractDataRanges()
classes= list(data_prop.keys())
super_classes_leaves=datagen.extractLeafClasses(data_prop)
super_classes_leaves[classes[1]]=sorted(super_classes_leaves[classes[1]])

ranges=datagen.extractRangeAssociatedDataPropreties(data_prop,data_prop_datatypes)
dfc,rows_list,isd=datagen.fetchRegionData()[0],datagen.fetchRegionData()[1],datagen.fetchRegionData()[2]
companies_lst=datagen.fetchCompanyDetails()
data_types={'str':str, 'float': float, 'int':int,'datetime.datetime': datetime.datetime}
excluded_cols=['Prefences', 'Personal_Data', 'Contact_Address']
#print(super_classes_leaves)
def extract_associated_datatypes():
    prop_datatypes=dict()
    for key in data_prop.keys():
        for i in range(len(data_prop[key])):
            if data_prop[key][i] not in list(ranges.keys()):
                try:
                    prop_datatypes[data_prop[key][i]]=str(data_prop_datatypes[data_prop[key][i]][0][2:-2])
                except:continue
    return prop_datatypes
prop_datatypes=extract_associated_datatypes()

region_code = str(projectConfig.get('default','region_code'))
type_of_person = str(projectConfig.get('default','type_of_person')).lower()
type_of_account = string.capwords(str(projectConfig.get('default','type_of_account')))
type_of_account= ''.join(type_of_account.split())

df_types = pd.read_csv('../output/outputDatatypes.csv')['types']

dfo=pd.read_excel('../output/'+region_code+'_'+type_of_person+type_of_account+'_data.xlsx',dtype=df_types.to_dict())
test_df=pd.read_excel('../lib/testCases.xlsx')
test_cols=list(test_df.columns)
def testCases():
	actualOutput,testStatus =[],[]
	for i, row in test_df.iterrows():
		colNam=row['Column']
		if row['Type of test'].lower() == 'range':
			vars()['col'+colNam+'RangeVals']=','.join(r for r in dfo[colNam].unique().tolist())
			#print(vars()['col'+colNam+'RangeVals'])
			actual=vars()['col'+colNam+'RangeVals']
			status= 'Passed' if(set(actual).issubset(set(row['Expected Output']))) else 'Failed'

		elif row['Type of test'].lower() == 'datatype':
			try:
				vars()['col'+colNam+'DataType']= str(type([x.to_pydatetime() for x in dfo[colNam].tolist()][random.randint(0,len(dfo)-1)])).split()[1][1:-2]
				actual=vars()['col'+colNam+'DataType']
			except:
				try:
					vars()['col'+colNam+'DataType']= str(type(dfo[colNam].tolist()[random.randint(0,len(dfo)-1)])).split()[1][1:-2]
					actual=vars()['col'+colNam+'DataType']
				except:actual='Column is absent for '+type_of_person
			status= 'Passed' if actual== row['Expected Output'] else 'Failed'

		elif row['Type of test'].lower()== 'maxlength':
			maxLen= max(dfo[colNam].apply(len))
			actual= maxLen
			status= 'Passed' if actual < row['Expected Output'] else 'Failed'

		elif row['Type of test'].lower()== 'value':
			values=dfo[colNam].unique().tolist()
			actual=row['Expected Output'] if row['Expected Output'] in values else 'Not Present'
			status= 'Passed' if(actual==row['Expected Output']) else 'Failed'

		actualOutput.append(actual)
		testStatus.append(status)
	test_df['Actual Output']= actualOutput
	test_df['Test status'] = testStatus
	test_df.to_excel('../output/testCases'+region_code+'_'+type_of_person+type_of_account+'.xlsx',index=False)

test= testCases()
