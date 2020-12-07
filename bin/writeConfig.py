#!/usr/bin/python3
# coding: utf-8
# packages
import warnings
warnings.filterwarnings("ignore")
import configparser


class WriteConfig:
	def __init__(self,projectConfig,myLogger):
		self.projectConfig=projectConfig
		self.myLogger=myLogger


	def changeinputs(self):
		try:
			self.region_code= input('Enter the region code you need to fetch the data(GB,IN,US): ')
			self.type_of_person= input('Enter the type of Person you want to extract data(Customer,Staff,Prospect):').lower()
			self.type_of_account= input('Enter the type of Account you want to extract data(Housing Loan,Car Loan,Personal Loan, Savings Account,Current Account,Time Deposit):')
			self.no_of_records= input('Enter the number of records you need to fetch: ')
			self.projectConfig['default']['type_of_person'] = self.type_of_person 
			self.projectConfig['default']['type_of_account'] = self.type_of_account 
			self.projectConfig['default']['no_of_records'] = self.no_of_records
			self.projectConfig['default']['region_code'] = self.region_code

			with open('../etc/application.ini', 'w') as configfile:
				self.projectConfig.write(configfile)

			print("Config details are changed sucessfully")
			print("logging out setup.py and please execute setup.py again...No need to change applicationInfo (click 'no') second time execution")
			sys.exit("logging out setup.py and please execute setup.py again...No need to change applicationInfo (click 'no') second time execution")

		except:
			pass
			
