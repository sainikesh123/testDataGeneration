import warnings
warnings.filterwarnings("ignore")
from owlready2 import *
from collections import defaultdict
import string
import ast 
import pandas as pd
from faker import Faker
import random
import names 
from datetime import date
from dateutil.relativedelta import relativedelta
from datetime import datetime
from datetime import timedelta
from barnum import gen_data
from fuzzywuzzy import fuzz
import uuid
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import rdflib
from mrz.generator.td3 import TD3CodeGenerator
import rstr
import os

try:
    onto_path.append('../lib/')
    onto = get_ontology("../lib/DM0.1.owl").load()
except:onto = get_ontology("../lib/DM0.1.owl").load()

onto.save()
onto.save(file = "../lib/xmldata", format = "rdfxml")
g=rdflib.Graph()
g.parse("../lib/xmldata", format="xml")
fake = Faker()
fake.seed_instance(4321)

bankDesignations=['Accountant','Bank Teller','Business Banking Officer','Consumer Loans Processor','Business Intelligence Manager','Customer Service Representative','Financial planner','Regulatory vendor program manager','Charted alternative investiment analyst','Charted financial analyst','Financial risk manager',
'Financial advisor','Marketing representative','Consumer Finance Assistant Manager','Assistant Branch Manager','Bilingual Client Services Representative','Custody Investment Specialist','Internal auditor','Branch manager','Loan officer','Data processing officer','Client Service Manager','Audit Manager','Bank Examiner','Business Banking Loan Administration Manager','Common Trust Fund Accountant']
bankNames=['JPMorgan Chase & Co','Wells Fargo','HSBC','Citigroup','Goldman Sachs Group','Morgan Stanley','BNP Paribas','UBS Group','DBS Group','Barclays','Deutsche Bank','Standard Chartered Bank','ICICI Bank']

data_types={'str':str, 'float': float, 'int':int}

df_keywords=pd.read_excel("../lib/dataPropKeywords.xlsx")
keywords={}
for i in df_keywords.columns:
    keywords[i]=[j for j in df_keywords[i] if type(j)==str]

class DataGenerator:
    def __init__(self,projectConfig,myLogger):
        self.projectConfig=projectConfig
        self.myLogger=myLogger 

    def extractDataProperties(self):
        propertiesGen = onto.data_properties()
        #self.classes=set()
        data_prop=defaultdict(list)
        for ontoProperty in propertiesGen:
            try:
                #self.classes.add(str(ontoProperty.domain[0])[6:])
                data_prop[str(ontoProperty.domain[0])[6:]].append(str(ontoProperty)[6:])
            except:continue
        for key in data_prop.keys():
            data_prop[key]=sorted(data_prop[key])
        return data_prop  

    def dataPropDct(self,data_prop):
        dataProp_dict=dict()
        for key in data_prop.keys():
            vars()[key.lower()+'_data_prop']=dict()
            for val in data_prop[key]:
                vars()[key.lower()+'_data_prop'][val]=val
            dataProp_dict.update(vars()[key.lower()+'_data_prop'])
        return dataProp_dict

    def extractDataRanges(self):
        propertiesGen = onto.data_properties()
        #self.classes=set()
        data_prop_datatypes=defaultdict(list)
        for ontoProperty in propertiesGen:
            try:
                #self.data_prop_datatypes[str(ontoProperty.range[0])[6:]].append(str(ontoProperty)[6:])
                data_prop_datatypes[str(ontoProperty)[6:]].append(str(ontoProperty.range[0])[6:])
            except:continue
        return data_prop_datatypes

    def extractLeafClasses(self,data_prop):
        r = list(g.query("""SELECT ?subject ?object
        WHERE { ?subject rdfs:subClassOf ?object }"""))
        dct=defaultdict(list)
        for i in r:
            dct[str(i[1])[1:]].append(str(i[0])[1:])
        keys_lst=list(dct.keys())
        class_keys=list(data_prop.keys())
        def subClasses(subclass_dct_keys):
            sub=[]
            if(set(subclass_dct_keys).issubset(set(keys_lst))):
                sk=subclass_dct_keys
                for k in sk:
                    sub.extend(dct[k])
            return sub

        def getLeafClasses():
            classes=[]
            subclass_dct=dict()
            for key in keys_lst:
                if key not in class_keys:
                    continue
                else:
                    subclass_dct[key]=dct[key]
                    leafclasses=subclass_dct[key]                        
            return subclass_dct
        subclass_dct=getLeafClasses()

        super_classes_leaves=dict()
        for key in subclass_dct.keys():
            leafclasses=subclass_dct[key]
            while True:
                if subClasses(leafclasses)!=[]:
                    leafclasses=subClasses(leafclasses)
                    continue
                else:
                    super_classes_leaves[key]=leafclasses
                    break
        return super_classes_leaves
    

    def extractRangeAssociatedDataPropreties(self,data_prop,data_prop_datatypes):
        ranges=dict()
        for key in data_prop.keys():
            for i in range(len(data_prop[key])):
                try:
                    ranges[data_prop[key][i]] = ast.literal_eval(data_prop_datatypes[data_prop[key][i]][0][:-1]) 
                except:continue
        return ranges


    def fetchRegionData(self):
        df=pd.read_excel('../lib/intermediateAddressGen.xlsx')
        reg_code=str(self.projectConfig.get('default','region_code')).upper()
        dfc=df[df.Region_Code==str(reg_code)]
        isd=str(re.sub('[^0-9]','',dfc['phone_number_with_isd'].tolist()[0].split('-')[0]))
        dfc=dfc.drop(columns=['address_line1','phone_number_with_isd','Language','Country_Name','Currency_Code','Currency_Name'])
        rows_list = dfc.values.tolist()
        return dfc,rows_list,isd

    def fetchCompanyDetails(self):
        df_comp_det=pd.read_csv('../lib/countryWiseCompanies_data.csv')
        #countries_lst= df_comp_det.country.tolist()
        df_country=df_comp_det[df_comp_det.region_code==str(self.projectConfig.get('default','region_code')).upper()][['name','industry']]
        companies_lst=df_country.values.tolist()
        return companies_lst

    def randomPhoneNumGenerator(self,isd):
        first = str(random.randint(100, 999))
        second = str(random.randint(1, 888)).zfill(3)

        last = (str(random.randint(1, 9998)).zfill(4))
        while last in ['1111', '2222', '3333', '4444', '5555', '6666', '7777', '8888']:
            last = (str(random.randint(1, 9998)).zfill(4))

        return '{}{}{}{}'.format(isd,first, second, last)

    def checkKeywords(self,actualName, key):
            f=0
            for w in keywords[key]:
                #print(fuzz.ratio(re.sub(r'[^A-Za-z0-9]','',actualName.lower()),re.sub(r'[^A-Za-z0-9]','',w.lower())),re.sub(r'[^A-Za-z0-9]','',actualName.lower()),re.sub(r'[^A-Za-z0-9]','',w.lower()))
                if re.sub(r'[^A-Za-z0-9]','',actualName.lower())== re.sub(r'[^A-Za-z0-9]','',w.lower()):
                    f=1 
                    break
            return f,actualName

    def getStaffDetails(self,dataProp_dct,companies_lst,classes,data_prop,birth):

        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'Company')[0]==1:
                companycolName=self.checkKeywords(key,'Company')[1]
                company = random.choice(bankNames)
                break
            else:company=None
        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'Sector')[0]==1:
                sectorcolName= self.checkKeywords(key,'Sector')[1]
                sector = 'Banking'
                break
            else:sector=None
        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'Designation')[0]==1:
                designationcolName= self.checkKeywords(key,'Designation')[1]
                designation = random.choice(bankDesignations)
                break
            else:designation=None
        while True:
            start_date=birth+relativedelta(years=random.randint(18,26))
            if start_date<datetime.today():
                for key in dataProp_dct.keys():
                    if self.checkKeywords(key,'From_Date')[0]==1:
                        fromDatecolName= self.checkKeywords(key,'From_Date')[1]
                        fromDate = start_date
                        break
                    else: fromDate=None
                break
            else:continue
        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'To_Date')[0]==1:
                toDatecolName= self.checkKeywords(key,'To_Date')[1]
                toDate = datetime.today()
                break
            else:toDate=None
        
        return [company,sector,designation,fromDate,toDate]


    def getProspectDetails(self,dataProp_dct,companies_lst,classes,data_prop,birth):
        lst=random.choice(companies_lst)
        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'Company')[0]==1:
                companycolName=self.checkKeywords(key,'Company')[1]
                company = lst[0]
                break
            else:company=None
        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'Sector')[0]==1:
                sectorcolName= self.checkKeywords(key,'Sector')[1]
                sector = lst[1]
                break
            else:sector=None
        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'Designation')[0]==1:
                designationcolName= self.checkKeywords(key,'Designation')[1]
                designation = gen_data.create_job_title()
                break
            else:designation=None
        while True:
            start_date=birth+relativedelta(years=random.randint(18,26))
            if start_date<datetime.today():
                for key in dataProp_dct.keys():
                    if self.checkKeywords(key,'From_Date')[0]==1:
                        fromDatecolName= self.checkKeywords(key,'From_Date')[1]
                        fromDate = start_date
                        break
                    else: fromDate=None
                break
            else:continue
        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'To_Date')[0]==1:
                toDatecolName= self.checkKeywords(key,'To_Date')[1]
                toDate = fromDate+timedelta(days=random.randrange((datetime.today() - fromDate).days))
                break
            else:toDate=None
        
        return [company,sector,designation,fromDate,toDate]

    def getCustomerDetails(self,dataProp_dct,companies_lst,classes,data_prop,birth):
        lst=random.choice(companies_lst)
        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'Company')[0]==1:
                companycolName=self.checkKeywords(key,'Company')[1]
                company = lst[0]
                break
            else:company=None
        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'Sector')[0]==1:
                sectorcolName= self.checkKeywords(key,'Sector')[1]
                sector = lst[1]
                break
            else:sector=None
        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'Designation')[0]==1:
                designationcolName= self.checkKeywords(key,'Designation')[1]
                designation = gen_data.create_job_title()
                break
            else:designation=None
        while True:
            start_date=birth+relativedelta(years=random.randint(18,26))
            if start_date<datetime.today():
                for key in dataProp_dct.keys():
                    if self.checkKeywords(key,'From_Date')[0]==1:
                        fromDatecolName= self.checkKeywords(key,'From_Date')[1]
                        fromDate = start_date
                        break
                    else: fromDate=None
                break
            else:continue
        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'To_Date')[0]==1:
                toDatecolName= self.checkKeywords(key,'To_Date')[1]
                toDate = fromDate+timedelta(days=random.randrange((datetime.today() - fromDate).days))
                break
            else:toDate=None
        
        return [company,sector,designation,fromDate,toDate]


    #data_prop[classes[1]][15]: ['Female', 'Male', 'Others', 'Transgender', 'Unknown'],
    #'Language': ['Arabic', 'English', 'Others'],
    #dataProp_dct['To_Date']: ['Mr', 'Mrs', 'Ms']}
    def getAccountDetails(self,dataProp_dct,ranges,classes,data_prop,birth):

        perc_active_accounts= float(self.projectConfig.get('default','perc_active_accounts'))
        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'Account_Status')[0]==1:
                accStatcolName=self.checkKeywords(key,'Account_Status')[1]  
                accStat= random.choices([ranges[accStatcolName][0],random.choice(ranges[accStatcolName][1:])],weights=[perc_active_accounts,1-perc_active_accounts])[0]  
                break
            else:accStat=None

        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'Account_Open_Date')[0]==1:
                accOpenDtcolName=self.checkKeywords(key,'Account_Open_Date')[1]
                accOpenDt= birth+relativedelta(years=15)+timedelta(days=random.randrange((datetime.today()+timedelta(days=-60)-(birth+relativedelta(years=15))).days))  
                break
            else:accOpenDt=None 

        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'Account_Closed_Date')[0]==1:
                accClsdDtcolName=self.checkKeywords(key,'Account_Closed_Date')[1]  
                accClsdDt = None if accStat=='Active' else accOpenDt+timedelta(days=60)+timedelta(days=random.randrange((datetime.today()-(accOpenDt+timedelta(days=60))).days))
                break
            else:accClsdDt=None 

        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'Account_Balance')[0]==1:
                accBalcolName=self.checkKeywords(key,'Account_Balance')[1]
                accBal= round(random.uniform(1000, 10**6),3)
                break
            else:accBal=None

        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'Account_Id')[0]==1:
                accIdcolName=self.checkKeywords(key,'Account_Id')[1]
                accId= ''.join(random.choices(string.digits, k=6))
                break
            else:accId=None

        return [accId,accStat,accOpenDt,accClsdDt,accBal]
    
    def details(self,dataProp_dct,classes,ranges,companies_lst,isd,data_prop,person,account_type,super_classes_leaves):
        perc_male_female=float(self.projectConfig.get('default','perc_male_female'))
        
        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'Gender')[0]==1:
                gencolName=self.checkKeywords(key,'Gender')[1]
                gend= random.choices([random.choice([ranges[gencolName][1],ranges[gencolName][0]]),random.choice(ranges[gencolName][2:])],weights=[perc_male_female,1-perc_male_female])[0]   
                break
            else:gend=None
        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'First_Name')[0]==1:
                fnamecolName=self.checkKeywords(key,'First_Name')[1]
                fname= names.get_first_name(gender=ranges[fnamecolName][1].lower()) if gend==ranges[gencolName][1] else names.get_first_name(gender=ranges[gencolName][0].lower()) if gend==ranges[gencolName][0] else random.choice([names.get_first_name(gender=ranges[gencolName][1].lower()),names.get_first_name(gender=ranges[gencolName][0].lower())])   
                break
            else:fname=None
        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'Middle_Name')[0]==1:
                middlecolName=self.checkKeywords(key,'Middle_Name')[1]
                mname= fake.name().split(' ')[0]   
                break
            else:mname=None
        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'Last_Name')[0]==1:
                lnamecolName=self.checkKeywords(key,'Last_Name')[1]
                lname= names.get_last_name()   
                break
            else:lname=None
        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'Email_Address')[0]==1:
                emailcolName=self.checkKeywords(key,'Email_Address')[1]
                email = fname.lower()+"_"+lname.lower()+re.search('@.*', fake.email()).group()   
                break
            else:email=None
        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'Date_of_Birth')[0]==1:
                dobcolName=self.checkKeywords(key,'Date_of_Birth')[1]
                dob = datetime(1960,1, 1) + timedelta(days=random.randrange((datetime(2000, 1, 1) - datetime(1960,1, 1)).days))  
                break
            else:dob=None
        age=relativedelta(date.today(),dob).years
        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'Title')[0]==1:
                titlecolName=self.checkKeywords(key,'Title')[1]
                title = ranges[titlecolName][0] if gend==ranges[gencolName][1] else ranges[titlecolName][2] if (age<=24 and gend==ranges[gencolName][0]) else ranges[titlecolName][1] if (age>24 and gend==ranges[gencolName][0]) else random.choice(ranges[titlecolName])  
                break
            else:title=None
        account_details= self.getAccountDetails(dataProp_dct,ranges,classes,data_prop,dob)
        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'Language')[0]==1:
                langcolName=self.checkKeywords(key,'Language')[1]
                lang = random.choice(ranges[langcolName])  
                break
            else:lang=None
        emp_details = eval("self.get%sDetails" % (person.capitalize()))(dataProp_dct,self.companies_lst,classes,data_prop,dob)

        if person.capitalize() not in list(data_prop.keys()):
            pass
        else:
            for key in dataProp_dct.keys():
                if self.checkKeywords(key,'Customer_Id')[0]==1:
                    custidcolName=self.checkKeywords(key,'Customer_Id')[1]
                    custid = uuid.uuid4().hex[:10]  
                    break
                else:custid=None
            for key in dataProp_dct.keys():
                if self.checkKeywords(key,'Relation_Customer_Id')[0]==1:
                    relcustidcolName=self.checkKeywords(key,'Customer_Id')[1]
                    relcustid = uuid.uuid4().hex[:10]  
                    break
                else:relcustid=None
            for key in dataProp_dct.keys():
                if self.checkKeywords(key,'Relation_Customer_Id')[0]==1:
                    relcustidcolName=self.checkKeywords(key,'Customer_Id')[1]
                    relcustid = uuid.uuid4().hex[:10]  
                    break
                else:relcustid=None
            for key in dataProp_dct.keys():
                if self.checkKeywords(key,'Relationship')[0]==1:
                    relationcolName=self.checkKeywords(key,'Relationship')[1]
                    relation = random.choice(['mother','father','sister','brother','grandmother','grandfather','sister-in-law','father-in-law','mother-in-law','brother-in-law','nephew','neice','cousin']) 
                    break
                else:relation=None
            
        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'Extension')[0]==1:
                extensioncolName=self.checkKeywords(key,'Extension')[1]
                extension = isd
                break
            else:extension=None
        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'Home_Phone_Number')[0]==1:
                homePhonecolName=self.checkKeywords(key,'Home_Phone_Number')[1]
                homePhone = self.randomPhoneNumGenerator(isd)
                break
            else:homePhone=None
        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'Mobile_Phone_Number')[0]==1:
                mobilePhonecolName=self.checkKeywords(key,'Mobile_Phone_Number')[1]
                mobilePhone = self.randomPhoneNumGenerator(isd)
                break
            else:mobilePhone=None
        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'Work_Phone_Number')[0]==1:
                workPhonecolName=self.checkKeywords(key,'Work_Phone_Number')[1]
                workPhone = self.randomPhoneNumGenerator(isd)
                break
            else:workPhone=None
        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'Address_Line1')[0]==1:
                addrLine1colName=self.checkKeywords(key,'Address_Line1')[1]
                addrLine1 = fake.street_address().split(' ')[1:]
                if len(addrLine1)<3:
                    addrLine1.append(str(random.randint(50,9999)))
                addrLine1=' '.join(i for i in addrLine1)
                break
            else:addrLine1=None
        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'Address_Type')[0]==1:
                addrTypecolName=self.checkKeywords(key,'Address_Type')[1]
                addrType = random.choice(['Permanent Address','Temporary Address'])
                break
            else:addrType=None
        for key in dataProp_dct.keys():
            if self.checkKeywords(key,'Locality')[0]==1:
                localitycolName=self.checkKeywords(key,'Locality')[1]
                locality = addrLine1
                break
            else:locality=None
                
        if person.capitalize() in list(data_prop.keys()):
            return {'Title':title,'First_Name':fname,'Middle_Name':mname,'Last_Name':lname,'Email_Address': email,'Gender':gend,'Date_of_Birth':dob,'Company':emp_details[0],'Sector':emp_details[1],'Designation':emp_details[2],'From_Date':emp_details[3],'To_Date':emp_details[4],'Customer_Id':custid,'Relation_Customer_Id':relcustid,'Relationship':relation,'Account_Id':account_details[0],'Account_Status':account_details[1],'Account_Open_Date':account_details[2],'Account_Closed_Date':account_details[3],'Account_Balance':account_details[4],'Extension':extension,'Home_Phone_Number':homePhone,'Mobile_Phone_Number':mobilePhone,'Work_Phone_Number':workPhone,'Address_Type':addrType,'Address_Line1':addrLine1,'Locality':locality,'Language':lang}
        elif super_classes_leaves[classes[1]][1]==person:
            return {'Title':title,'First_Name':fname,'Middle_Name':mname,'Last_Name':lname,'Email_Address': email,'Gender':gend,'Date_of_Birth':dob,'Company':emp_details[0],'Sector':emp_details[1],'Designation':emp_details[2],'From_Date':emp_details[3],'To_Date':emp_details[4],'Account_Id':account_details[0],'Account_Status':account_details[1],'Account_Open_Date':account_details[2],'Account_Closed_Date':account_details[3],'Account_Balance':account_details[4],'Extension':extension,'Home_Phone_Number':homePhone,'Mobile_Phone_Number':mobilePhone,'Work_Phone_Number':workPhone,'Address_Type':addrType,'Address_Line1':addrLine1,'Locality':locality,'Language':lang}
        else:
            return {'Title':title,'First_Name':fname,'Middle_Name':mname,'Last_Name':lname,'Email_Address': email,'Gender':gend,'Date_of_Birth':dob,'Company':emp_details[0],'Sector':emp_details[1],'Designation':emp_details[2],'From_Date':emp_details[3],'To_Date':emp_details[4],'Account_Id':account_details[0],'Account_Status':account_details[1],'Account_Open_Date':account_details[2],'Account_Closed_Date':account_details[3],'Account_Balance':account_details[4],'Extension':extension,'Home_Phone_Number':homePhone,'Mobile_Phone_Number':mobilePhone,'Work_Phone_Number':workPhone,'Address_Type':addrType,'Address_Line1':addrLine1,'Locality':locality,'Language':lang}


    def assignNationalId(self,dft):
        dft['National_Id_Type']='Passport'
        dft['National_Id']='None'
        #Genre. Male: 'M', Female: 'F' or Undefined: 'X', "<" or ""
        #YYMMDD
        for i in range(len(dft)):
            expire_date=fake.date_between_dates(date_start=datetime.today(), date_end=datetime.today()+relativedelta(years=11))
            expire_date_format=str(expire_date)[2:4]+str(expire_date).split('-')[1]+str(expire_date).split('-')[2]
            birth_date_format=str(str(dft.Date_of_Birth.iloc[i])[2:4]+str(dft.Date_of_Birth.iloc[i]).split('-')[1]+str(dft.Date_of_Birth.iloc[i]).split('-')[2]).split(' ')[0]
            gen='M' if dft.Gender.iloc[i]=='Male' else 'F' if dft.Gender.iloc[i]=='Female' else ''
            #try:
            dft.National_Id.iloc[i] = TD3CodeGenerator("P",dft.Country_iso3.iloc[i],dft.First_Name.iloc[i], dft.Last_Name.iloc[i],rstr.xeger('[a-zA-Z0-9]{2}[0-9]{5,7}$'),dft.Country_iso3.iloc[i],birth_date_format,gen,expire_date_format,rstr.xeger('[a-zA-Z0-9]{2}[0-9]{5,7}$'))
            #except:continue
        return dft

    def main(self):
        self.data_prop=self.extractDataProperties()
        self.classes= list(self.data_prop.keys())
        self.dataProp_dct= self.dataPropDct(self.data_prop)
        self.data_prop_datatypes=self.extractDataRanges()
        self.ranges=self.extractRangeAssociatedDataPropreties(self.data_prop,self.data_prop_datatypes)
        
        self.dfc,self.rows_list,self.isd=self.fetchRegionData()[0],self.fetchRegionData()[1],self.fetchRegionData()[2]
        self.companies_lst=self.fetchCompanyDetails()
        self.super_classes_leaves=self.extractLeafClasses(self.data_prop)
        self.super_classes_leaves[self.classes[1]]=sorted(self.super_classes_leaves[self.classes[1]])
        d = dict()
        d['details'] = self.details
        self.type_of_person= str(self.projectConfig.get('default','type_of_person')).lower()
        self.type_of_account = string.capwords(str(self.projectConfig.get('default','type_of_account')))
        self.type_of_account= ''.join(self.type_of_account.split())
        while True:
            try:
                col=[list(d[k](self.dataProp_dct,self.classes,self.ranges,self.companies_lst,self.isd,self.data_prop,self.type_of_person,self.type_of_account,self.super_classes_leaves).keys()) for k in d.keys()][0]
                break
            except:pass
        mas_lst=[]
        processes=[]
        start=time.time()
        def fetchData():
            try:
                deep_list = [list(d[k](self.dataProp_dct,self.classes,self.ranges,self.companies_lst,self.isd,self.data_prop,self.type_of_person,self.type_of_account,self.super_classes_leaves).values()) for k in d.keys()]
            except:return
            row = [item for sublist in deep_list for item in sublist]
            row.extend(random.choice(self.rows_list))
            mas_lst.append(row)
            print(row)
        with ThreadPoolExecutor(max_workers=100) as executor:
            for _ in range(2*int(self.projectConfig.get('default','no_of_records'))):
                processes.append(executor.submit(fetchData))

        print(time.time()-start)
        print(len(mas_lst),' no. of records')
        col.extend(list(self.dfc.columns))
        dft=pd.DataFrame(mas_lst,columns=col)
        dft=self.assignNationalId(dft)
        region_code=dft.Region_Code.tolist()[0]
        dft=dft.drop(columns=['Country_iso3','Region_Code'])
        if os.path.exists('../output/'+region_code+'_'+self.type_of_person+self.type_of_account+'_data.xlsx'):
            dfp=pd.read_excel('../output/'+region_code+'_'+self.type_of_person+self.type_of_account+'_data.xlsx')
            dft=pd.concat([dfp,dft])
        #dft.drop_duplicates(subset=list(dft.columns))
        def extractAssociatedDatatypes():
            prop_datatypes=dict()
            for key in self.data_prop.keys():
                for i in range(len(self.data_prop[key])):
                    if self.data_prop[key][i] not in list(self.ranges.keys()):
                        try:
                            prop_datatypes[self.data_prop[key][i]]=str(self.data_prop_datatypes[self.data_prop[key][i]][0][2:-2])
                        except:continue
            return prop_datatypes
        self.prop_datatypes=extractAssociatedDatatypes()
        excluded_cols=['Prefences', 'Personal_Data', 'Contact_Address','Phone_Number']

        if self.super_classes_leaves[self.classes[1]][0].lower()==self.type_of_person:
            for key in self.prop_datatypes.keys():
                if self.prop_datatypes[key] in list(data_types.keys()) and key not in excluded_cols:
                    dft[key]=dft[key].astype(self.prop_datatypes[key])
                    #print(key,':', type(dft[key].tolist()[0]),'actual:',self.prop_datatypes[key])
        else:
            customer_specific= self.data_prop[self.super_classes_leaves[self.classes[1]][0]]
            customer_specific.extend(excluded_cols)
            for key in self.prop_datatypes:
                if key not in customer_specific and self.prop_datatypes[key] in list(data_types.keys()):
                    dft[key]=dft[key].astype(self.prop_datatypes[key])
                    #print(key,':', type(dft[key].tolist()[0]),'actual:',self.prop_datatypes[key])
        dft.dtypes.to_frame('types').to_csv('../output/outputDatatypes.csv')
        dft.to_excel('../output/'+region_code+'_'+self.type_of_person+self.type_of_account+'_data.xlsx',index=False)

        