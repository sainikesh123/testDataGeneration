				                 Python package for 
=======================================================================================================================================================
Version             :test data generation
Any licenses needed :Nil
Licensed to         :Maveric systems
Developer           :
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
'test data generation' is the packaged file to create synthetic data

Prerequisites
-------------------------------------------------------------------------------------------------------------------------------------------------------
Step1 - Download and install python-3.6.5 version
=======================================================================================================================================================
Step1 - bin folder contains all the .py files for executing
Step2 - etc folder is having 'application.ini' file, there we can give inputs directly
Step3 - lib folder contains 
	1.'requirement.txt' file which contain all the packages which is used for this program
	2.'DMO.1.owl' file contains properties to be extracted using dataGenerations.py file
	3.'intermediateAddressGen.xlsx' excel file is used for region_code and address generator
	4.'countryWiseCompanies.csv' file is used for companies and their sectors
	5.'xmldata' is an xml file contains properties to be extracted using dataGenerations.py file
Step4 - log folder is for creating log for the program
Step5 - output folder is for storing outputs for the program

Program structure
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
Step1 - 'createConfigAndlog.py' is for reading 'application.ini' file and for creating logs
Step2 - 'dataGenerations.py' is for generating data
Step3 - 'packageInstallation.py' is for installing all the necessary packages (this packages will sucessfully install if only you have admin privilege)
Step4 - 'testDataGenerated.py' is used for unittesting
Step5 - 'setup.py' is for executing all the programs which is mentioned above in one execution
Step6 - 'writeConfig.py' is used changing configuration details in 'application.ini'
=============================================End of Process=============================================


