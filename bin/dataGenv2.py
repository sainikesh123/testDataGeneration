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
import uuid
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import rdflib
from mrz.generator.td3 import TD3CodeGenerator
import rstr
import os

try:
    onto_path.append('../lib/')
    onto = get_ontology("../lib/DM0.2.owl").load()
except:onto = get_ontology("../lib/DM0.2.owl").load()

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


    def getStaffDetails(self,companies_lst,classes,data_prop,birth):
        vars()[data_prop[classes[1]][4]]=random.choice(bankNames)
        vars()[data_prop[classes[1]][-5]]= 'Banking'
        vars()[data_prop[classes[1]][7]]=random.choice(bankDesignations)
        while True:
            start_date=birth+relativedelta(years=random.randint(18,26))
            if start_date<datetime.today():
                vars()[data_prop[classes[1]][12]]=start_date
                break
            else:continue
        vars()[data_prop[classes[1]][-2]]= datetime.today()
        return [vars()[data_prop[classes[1]][4]],vars()[data_prop[classes[1]][-5]],vars()[data_prop[classes[1]][7]],vars()[data_prop[classes[1]][12]],vars()[data_prop[classes[1]][-2]]]


    def getProspectDetails(self,companies_lst,classes,data_prop,birth):
        lst=random.choice(companies_lst)
        vars()[data_prop[classes[1]][4]]= lst[0]                
        vars()[data_prop[classes[1]][-5]]=lst[1]
        vars()[data_prop[classes[1]][7]]=gen_data.create_job_title()
        while True:
            start_date=birth+relativedelta(years=random.randint(18,26))
            if start_date<datetime.today():
                vars()[data_prop[classes[1]][12]]=start_date
                break
            else:continue
        vars()[data_prop[classes[1]][-2]]=vars()[data_prop[classes[1]][14]]+timedelta(days=random.randrange((datetime.today() - vars()[data_prop[classes[1]][14]]).days))
        return [vars()[data_prop[classes[1]][4]],vars()[data_prop[classes[1]][-5]],vars()[data_prop[classes[1]][7]],vars()[data_prop[classes[1]][12]],vars()[data_prop[classes[1]][-2]]]

    def getCustomerDetails(self,companies_lst,classes,data_prop,birth):
        lst=random.choice(companies_lst)
        vars()[data_prop[classes[1]][4]]= lst[0]                 #gen_data.create_company_name(biz_type="Generic")
        vars()[data_prop[classes[1]][-5]]=lst[1]
        vars()[data_prop[classes[1]][7]]=gen_data.create_job_title()
        while True:
            start_date=birth+relativedelta(years=random.randint(18,26))
            if start_date<datetime.today():
                vars()[data_prop[classes[1]][12]]=start_date
                break
            else:continue
        vars()[data_prop[classes[1]][-2]]=vars()[data_prop[classes[1]][12]]+timedelta(days=random.randrange((datetime.today() - vars()[data_prop[classes[1]][12]]).days))
        return [vars()[data_prop[classes[1]][4]],vars()[data_prop[classes[1]][-5]],vars()[data_prop[classes[1]][7]],vars()[data_prop[classes[1]][12]],vars()[data_prop[classes[1]][-2]]]

    #data_prop[classes[1]][15]: ['Female', 'Male', 'Others', 'Transgender', 'Unknown'],
    #'Language': ['Arabic', 'English', 'Others'],
    #data_prop[classes[1]][-2]: ['Mr', 'Mrs', 'Ms']}
    def getAccountDetails(self,ranges,classes,data_prop,birth):
        perc_active_accounts= float(self.projectConfig.get('default','perc_active_accounts'))
        vars()[data_prop[classes[0]][-1]] = random.choices([ranges[data_prop[classes[0]][-1]][0],random.choice(ranges[data_prop[classes[0]][-1]][1:])],weights=[perc_active_accounts,1-perc_active_accounts])[0]
        vars()[data_prop[classes[0]][3]]= birth+relativedelta(years=15)+timedelta(days=random.randrange((datetime.today()+timedelta(days=-60)-(birth+relativedelta(years=15))).days))
        vars()[data_prop[classes[0]][1]]= None if vars()[data_prop[classes[0]][-1]]=='Active' else vars()[data_prop[classes[0]][3]]+timedelta(days=60)+timedelta(days=random.randrange((datetime.today()-(vars()[data_prop[classes[0]][3]]+timedelta(days=60))).days))
        vars()[data_prop[classes[0]][0]]= round(random.uniform(1000, 10**6),3)
        vars()[data_prop[classes[0]][2]] = ''.join(random.choices(string.digits, k=6))
        return [vars()[data_prop[classes[0]][2]],vars()[data_prop[classes[0]][-1]],vars()[data_prop[classes[0]][3]],vars()[data_prop[classes[0]][1]],vars()[data_prop[classes[0]][0]]]
    
    def details(self,classes,ranges,companies_lst,isd,data_prop,person,account_type,super_classes_leaves):
        perc_male_female=float(self.projectConfig.get('default','perc_male_female'))
        
        vars()[data_prop[classes[1]][13]] = random.choices([random.choice([ranges[data_prop[classes[1]][13]][1],ranges[data_prop[classes[1]][13]][0]]),random.choice(ranges[data_prop[classes[1]][13]][2:])],weights=[perc_male_female,1-perc_male_female])[0]   
        vars()[data_prop[classes[1]][11]] = names.get_first_name(gender=ranges[data_prop[classes[1]][13]][1].lower()) if vars()[data_prop[classes[1]][13]]==ranges[data_prop[classes[1]][13]][1] else names.get_first_name(gender=ranges[data_prop[classes[1]][13]][0].lower()) if vars()[data_prop[classes[1]][13]]==ranges[data_prop[classes[1]][13]][0] else random.choice([names.get_first_name(gender=ranges[data_prop[classes[1]][13]][1].lower()),names.get_first_name(gender=ranges[data_prop[classes[1]][13]][0].lower())])
        vars()[data_prop[classes[1]][19]] = fake.name().split(' ')[0]
        vars()[data_prop[classes[1]][17]] = names.get_last_name()
        vars()[data_prop[classes[1]][8]] = vars()[data_prop[classes[1]][11]].lower()+"_"+vars()[data_prop[classes[1]][17]].lower()+re.search('@.*', fake.email()).group()
        vars()[data_prop[classes[1]][6]]= datetime(1960,1, 1) + timedelta(days=random.randrange((datetime(2000, 1, 1) - datetime(1960,1, 1)).days))
        age=relativedelta(date.today(),vars()[data_prop[classes[1]][6]]).years
        vars()[data_prop[classes[1]][-3]]=ranges[data_prop[classes[1]][-3]][0] if vars()[data_prop[classes[1]][13]]==ranges[data_prop[classes[1]][13]][1] else ranges[data_prop[classes[1]][-3]][2] if (age<=24 and vars()[data_prop[classes[1]][13]]==ranges[data_prop[classes[1]][13]][0]) else ranges[data_prop[classes[1]][-3]][1] if (age>24 and vars()[data_prop[classes[1]][13]]==ranges[data_prop[classes[1]][13]][0]) else random.choice(ranges[data_prop[classes[1]][-3]])
        account_details= self.getAccountDetails(ranges,classes,data_prop,vars()[data_prop[classes[1]][6]])
        vars()[data_prop[classes[1]][16]] = random.choice(ranges[data_prop[classes[1]][16]])
        emp_details = eval("self.get%sDetails" % (person.capitalize()))(self.companies_lst,classes,data_prop,vars()[data_prop[classes[1]][6]])
        if person.capitalize() not in list(data_prop.keys()):
            pass
        else:
            vars()[data_prop[classes[2]][0]] =uuid.uuid4().hex[:10]
            vars()[data_prop[classes[2]][2]]=uuid.uuid4().hex[:10]
            vars()[data_prop[classes[2]][-1]]=random.choice(['mother','father','sister','brother','grandmother','grandfather','sister-in-law','father-in-law','mother-in-law','brother-in-law','nephew','neice','cousin'])
        #Deposit=random.choice(['Current account','Savings account','Time deposit'])
        #loan=random.choice(['Car loan','Housing loan','Personal loan'])

        vars()[data_prop[classes[1]][10]]= isd
        vars()[data_prop[classes[1]][14]]=self.randomPhoneNumGenerator(isd)
        vars()[data_prop[classes[1]][20]]=self.randomPhoneNumGenerator(isd)
        vars()[data_prop[classes[1]][-1]]=self.randomPhoneNumGenerator(isd)
        vars()[data_prop[classes[1]][0]]=fake.street_address().split(' ')[1:]
        vars()[data_prop[classes[1]][2]]= random.choice(['Permanent Address','Temporary Address'])
        if len(vars()[data_prop[classes[1]][0]])<3:
            vars()[data_prop[classes[1]][0]].append(str(random.randint(50,9999)))
        vars()[data_prop[classes[1]][0]]=' '.join(i for i in vars()[data_prop[classes[1]][0]])
        vars()[data_prop[classes[1]][18]]= vars()[data_prop[classes[1]][0]]
        if person.capitalize() in list(data_prop.keys()):
            return {data_prop[classes[1]][-3]:vars()[data_prop[classes[1]][-3]],data_prop[classes[1]][11]:vars()[data_prop[classes[1]][11]],data_prop[classes[1]][19]:vars()[data_prop[classes[1]][19]],data_prop[classes[1]][17]:vars()[data_prop[classes[1]][17]],data_prop[classes[1]][8]: vars()[data_prop[classes[1]][8]],data_prop[classes[1]][13]:vars()[data_prop[classes[1]][13]],data_prop[classes[1]][6]:vars()[data_prop[classes[1]][6]],data_prop[classes[1]][4]:emp_details[0],data_prop[classes[1]][-5]:emp_details[1],data_prop[classes[1]][7]:emp_details[2],data_prop[classes[1]][12]:emp_details[3],data_prop[classes[1]][-2]:emp_details[4],data_prop[classes[2]][0]:vars()[data_prop[classes[2]][0]],data_prop[classes[2]][2]:vars()[data_prop[classes[2]][2]],data_prop[classes[2]][-1]:vars()[data_prop[classes[2]][-1]],data_prop[classes[0]][2]:account_details[0],data_prop[classes[0]][-1]:account_details[1],data_prop[classes[0]][3]:account_details[2],data_prop[classes[0]][1]:account_details[3],data_prop[classes[0]][0]:account_details[4],data_prop[classes[1]][10]:vars()[data_prop[classes[1]][10]],data_prop[classes[1]][14]:vars()[data_prop[classes[1]][14]],data_prop[classes[1]][20]:vars()[data_prop[classes[1]][20]],data_prop[classes[1]][-1]:vars()[data_prop[classes[1]][-1]],data_prop[classes[1]][2]:vars()[data_prop[classes[1]][2]],data_prop[classes[1]][0]:vars()[data_prop[classes[1]][0]],data_prop[classes[1]][18]:vars()[data_prop[classes[1]][18]],data_prop[classes[1]][16]:vars()[data_prop[classes[1]][16]]}
        elif super_classes_leaves[classes[1]][1]==person:
            return {data_prop[classes[1]][-3]:vars()[data_prop[classes[1]][-3]],data_prop[classes[1]][11]:vars()[data_prop[classes[1]][11]],data_prop[classes[1]][19]:vars()[data_prop[classes[1]][19]],data_prop[classes[1]][17]:vars()[data_prop[classes[1]][17]],data_prop[classes[1]][8]: vars()[data_prop[classes[1]][8]],data_prop[classes[1]][13]:vars()[data_prop[classes[1]][13]],data_prop[classes[1]][6]:vars()[data_prop[classes[1]][6]],data_prop[classes[1]][4]:emp_details[0],data_prop[classes[1]][-5]:emp_details[1],data_prop[classes[1]][7]:emp_details[2],data_prop[classes[1]][12]:emp_details[3],data_prop[classes[1]][-2]:emp_details[4],data_prop[classes[0]][2]:account_details[0],data_prop[classes[0]][-1]:account_details[1],data_prop[classes[0]][3]:account_details[2],data_prop[classes[0]][1]:account_details[3],data_prop[classes[0]][0]:account_details[4],data_prop[classes[1]][10]:vars()[data_prop[classes[1]][10]],data_prop[classes[1]][14]:vars()[data_prop[classes[1]][14]],data_prop[classes[1]][20]:vars()[data_prop[classes[1]][20]],data_prop[classes[1]][-1]:vars()[data_prop[classes[1]][-1]],data_prop[classes[1]][2]:vars()[data_prop[classes[1]][2]],data_prop[classes[1]][0]:vars()[data_prop[classes[1]][0]],data_prop[classes[1]][18]:vars()[data_prop[classes[1]][18]],data_prop[classes[1]][16]:vars()[data_prop[classes[1]][16]]}
        else:
            return {data_prop[classes[1]][-3]:vars()[data_prop[classes[1]][-3]],data_prop[classes[1]][11]:vars()[data_prop[classes[1]][11]],data_prop[classes[1]][19]:vars()[data_prop[classes[1]][19]],data_prop[classes[1]][17]:vars()[data_prop[classes[1]][17]],data_prop[classes[1]][8]: vars()[data_prop[classes[1]][8]],data_prop[classes[1]][13]:vars()[data_prop[classes[1]][13]],data_prop[classes[1]][6]:vars()[data_prop[classes[1]][6]],data_prop[classes[1]][4]:emp_details[0],data_prop[classes[1]][-5]:emp_details[1],data_prop[classes[1]][7]:emp_details[2],data_prop[classes[1]][12]:emp_details[3],data_prop[classes[1]][-2]:emp_details[4],data_prop[classes[0]][2]:account_details[0],data_prop[classes[0]][-1]:account_details[1],data_prop[classes[0]][3]:account_details[2],data_prop[classes[0]][1]:account_details[3],data_prop[classes[0]][0]:account_details[4],data_prop[classes[1]][10]:vars()[data_prop[classes[1]][10]],data_prop[classes[1]][14]:vars()[data_prop[classes[1]][14]],data_prop[classes[1]][20]:vars()[data_prop[classes[1]][20]],data_prop[classes[1]][-1]:vars()[data_prop[classes[1]][-1]],data_prop[classes[1]][2]:vars()[data_prop[classes[1]][2]],data_prop[classes[1]][0]:vars()[data_prop[classes[1]][0]],data_prop[classes[1]][18]:vars()[data_prop[classes[1]][18]],data_prop[classes[1]][16]:vars()[data_prop[classes[1]][16]]}


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
        col=[list(d[k](self.classes,self.ranges,self.companies_lst,self.isd,self.data_prop,self.type_of_person,self.type_of_account,self.super_classes_leaves).keys()) for k in d.keys()][0]

        mas_lst=[]
        processes=[]
        start=time.time()
        def fetchData():
            try:
                deep_list = [list(d[k](self.classes,self.ranges,self.companies_lst,self.isd,self.data_prop,self.type_of_person,self.type_of_account,self.super_classes_leaves).values()) for k in d.keys()]
            except:return
            row = [item for sublist in deep_list for item in sublist]
            row.extend(random.choice(self.rows_list))
            mas_lst.append(row)
            print(row)
        with ThreadPoolExecutor(max_workers=100) as executor:
            for _ in range(int(self.projectConfig.get('default','no_of_records'))):
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
        excluded_cols=['Prefences', 'Personal_Data', 'Contact_Address']

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
        