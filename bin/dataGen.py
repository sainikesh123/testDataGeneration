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


    def getStaffDetails(self,dataProp_dct,companies_lst,classes,data_prop,birth):
        vars()[dataProp_dct['Company']]=random.choice(bankNames)
        vars()[dataProp_dct['Sector']]= 'Banking'
        vars()[dataProp_dct['Designation']]=random.choice(bankDesignations)
        while True:
            start_date=birth+relativedelta(years=random.randint(18,26))
            if start_date<datetime.today():
                vars()[dataProp_dct['From_Date']]=start_date
                break
            else:continue
        vars()[dataProp_dct['To_Date']]= datetime.today()
        return [vars()[dataProp_dct['Company']],vars()[dataProp_dct['Sector']],vars()[dataProp_dct['Designation']],vars()[dataProp_dct['From_Date']],vars()[dataProp_dct['To_Date']]]


    def getProspectDetails(self,dataProp_dct,companies_lst,classes,data_prop,birth):
        lst=random.choice(companies_lst)
        vars()[dataProp_dct['Company']]= lst[0]                 #gen_data.create_company_name(biz_type="Generic")
        vars()[dataProp_dct['Sector']]=lst[1]
        vars()[dataProp_dct['Designation']]=gen_data.create_job_title()
        while True:
            start_date=birth+relativedelta(years=random.randint(18,26))
            if start_date<datetime.today():
                vars()[dataProp_dct['From_Date']]=start_date
                break
            else:continue
        vars()[dataProp_dct['To_Date']]=vars()[dataProp_dct['From_Date']]+timedelta(days=random.randrange((datetime.today() - vars()[dataProp_dct['From_Date']]).days))
        return [vars()[dataProp_dct['Company']],vars()[dataProp_dct['Sector']],vars()[dataProp_dct['Designation']],vars()[dataProp_dct['From_Date']],vars()[dataProp_dct['To_Date']]]

    def getCustomerDetails(self,dataProp_dct,companies_lst,classes,data_prop,birth):
        lst=random.choice(companies_lst)
        vars()[dataProp_dct['Company']]= lst[0]                 #gen_data.create_company_name(biz_type="Generic")
        vars()[dataProp_dct['Sector']]=lst[1]
        vars()[dataProp_dct['Designation']]=gen_data.create_job_title()
        while True:
            start_date=birth+relativedelta(years=random.randint(18,26))
            if start_date<datetime.today():
                vars()[dataProp_dct['From_Date']]=start_date
                break
            else:continue
        vars()[dataProp_dct['To_Date']]=vars()[dataProp_dct['From_Date']]+timedelta(days=random.randrange((datetime.today() - vars()[dataProp_dct['From_Date']]).days))
        return [vars()[dataProp_dct['Company']],vars()[dataProp_dct['Sector']],vars()[dataProp_dct['Designation']],vars()[dataProp_dct['From_Date']],vars()[dataProp_dct['To_Date']]]

    #data_prop[classes[1]][15]: ['Female', 'Male', 'Others', 'Transgender', 'Unknown'],
    #'Language': ['Arabic', 'English', 'Others'],
    #dataProp_dct['To_Date']: ['Mr', 'Mrs', 'Ms']}
    def getAccountDetails(self,dataProp_dct,ranges,classes,data_prop,birth):
        perc_active_accounts= float(self.projectConfig.get('default','perc_active_accounts'))
        vars()[dataProp_dct['Account_Status']]= random.choices([ranges[dataProp_dct['Account_Status']][0],random.choice(ranges[dataProp_dct['Account_Status']][1:])],weights=[perc_active_accounts,1-perc_active_accounts])[0]
        vars()[dataProp_dct['Account_Open_Date']]= birth+relativedelta(years=15)+timedelta(days=random.randrange((datetime.today()+timedelta(days=-60)-(birth+relativedelta(years=15))).days))
        vars()[dataProp_dct['Account_Closed_Date']]= None if vars()[dataProp_dct['Account_Status']]=='Active' else vars()[dataProp_dct['Account_Open_Date']]+timedelta(days=60)+timedelta(days=random.randrange((datetime.today()-(vars()[dataProp_dct['Account_Open_Date']]+timedelta(days=60))).days))
        vars()[dataProp_dct['Account_Balance']]= round(random.uniform(1000, 10**6),3)
        vars()[dataProp_dct['Account_Id']]= ''.join(random.choices(string.digits, k=6))
        return [vars()[dataProp_dct['Account_Id']],vars()[dataProp_dct['Account_Status']],vars()[dataProp_dct['Account_Open_Date']],vars()[dataProp_dct['Account_Closed_Date']],vars()[dataProp_dct['Account_Balance']]]
    
    def details(self,dataProp_dct,classes,ranges,companies_lst,isd,data_prop,person,account_type,super_classes_leaves):
        perc_male_female=float(self.projectConfig.get('default','perc_male_female'))
        
        vars()[dataProp_dct['Gender']] = random.choices([random.choice([ranges[dataProp_dct['Gender']][1],ranges[dataProp_dct['Gender']][0]]),random.choice(ranges[dataProp_dct['Gender']][2:])],weights=[perc_male_female,1-perc_male_female])[0]   
        vars()[dataProp_dct['First_Name']] = names.get_first_name(gender=ranges[dataProp_dct['Gender']][1].lower()) if vars()[dataProp_dct['Gender']]==ranges[dataProp_dct['Gender']][1] else names.get_first_name(gender=ranges[dataProp_dct['Gender']][0].lower()) if vars()[dataProp_dct['Gender']]==ranges[dataProp_dct['Gender']][0] else random.choice([names.get_first_name(gender=ranges[dataProp_dct['Gender']][1].lower()),names.get_first_name(gender=ranges[dataProp_dct['Gender']][0].lower())])
        vars()[dataProp_dct['Middle_Name']] = fake.name().split(' ')[0]
        vars()[dataProp_dct['Last_Name']] = names.get_last_name()
        vars()[dataProp_dct['Email_Address']] = vars()[dataProp_dct['First_Name']].lower()+"_"+vars()[dataProp_dct['Last_Name']].lower()+re.search('@.*', fake.email()).group()
        vars()[dataProp_dct['Date_of_Birth']]= datetime(1960,1, 1) + timedelta(days=random.randrange((datetime(2000, 1, 1) - datetime(1960,1, 1)).days))
        age=relativedelta(date.today(),vars()[dataProp_dct['Date_of_Birth']]).years
        vars()[dataProp_dct['Title']]=ranges[dataProp_dct['Title']][0] if vars()[dataProp_dct['Gender']]==ranges[dataProp_dct['Gender']][1] else ranges[dataProp_dct['Title']][2] if (age<=24 and vars()[dataProp_dct['Gender']]==ranges[dataProp_dct['Gender']][0]) else ranges[dataProp_dct['Title']][1] if (age>24 and vars()[dataProp_dct['Gender']]==ranges[dataProp_dct['Gender']][0]) else random.choice(ranges[dataProp_dct['Title']])
        account_details= self.getAccountDetails(dataProp_dct,ranges,classes,data_prop,vars()[dataProp_dct['Date_of_Birth']])
        vars()[dataProp_dct['Language']] = random.choice(ranges[dataProp_dct['Language']])
        emp_details = eval("self.get%sDetails" % (person.capitalize()))(dataProp_dct,self.companies_lst,classes,data_prop,vars()[dataProp_dct['Date_of_Birth']])
        if person.capitalize() not in list(data_prop.keys()):
            pass
        else:
            vars()[dataProp_dct['Customer_Id']] =uuid.uuid4().hex[:10]
            vars()[dataProp_dct['Relation_Customer_Id']]=uuid.uuid4().hex[:10]
            vars()[dataProp_dct['Relationship']]=random.choice(['mother','father','sister','brother','grandmother','grandfather','sister-in-law','father-in-law','mother-in-law','brother-in-law','nephew','neice','cousin'])
        #Deposit=random.choice(['Current account','Savings account','Time deposit'])
        #loan=random.choice(['Car loan','Housing loan','Personal loan'])

        vars()[dataProp_dct['Extension']]= isd
        vars()[dataProp_dct['Home_Phone_Number']]=self.randomPhoneNumGenerator(isd)
        vars()[dataProp_dct['Mobile_Phone_Number']]=self.randomPhoneNumGenerator(isd)
        vars()[dataProp_dct['Work_Phone_Number']]=self.randomPhoneNumGenerator(isd)
        vars()[dataProp_dct['Address_Line1']]=fake.street_address().split(' ')[1:]
        vars()[dataProp_dct['Address_Type']]= random.choice(['Permanent Address','Temporary Address'])
        if len(vars()[dataProp_dct['Address_Line1']])<3:
            vars()[dataProp_dct['Address_Line1']].append(str(random.randint(50,9999)))
        vars()[dataProp_dct['Address_Line1']]=' '.join(i for i in vars()[dataProp_dct['Address_Line1']])
        vars()[dataProp_dct['Locality']]= vars()[dataProp_dct['Address_Line1']]
        if person.capitalize() in list(data_prop.keys()):
            return {dataProp_dct['Title']:vars()[dataProp_dct['Title']],dataProp_dct['First_Name']:vars()[dataProp_dct['First_Name']],dataProp_dct['Middle_Name']:vars()[dataProp_dct['Middle_Name']],dataProp_dct['Last_Name']:vars()[dataProp_dct['Last_Name']],dataProp_dct['Email_Address']: vars()[dataProp_dct['Email_Address']],dataProp_dct['Gender']:vars()[dataProp_dct['Gender']],dataProp_dct['Date_of_Birth']:vars()[dataProp_dct['Date_of_Birth']],dataProp_dct['Company']:emp_details[0],dataProp_dct['Sector']:emp_details[1],dataProp_dct['Designation']:emp_details[2],dataProp_dct['From_Date']:emp_details[3],dataProp_dct['To_Date']:emp_details[4],dataProp_dct['Customer_Id']:vars()[dataProp_dct['Customer_Id']],dataProp_dct['Relation_Customer_Id']:vars()[dataProp_dct['Relation_Customer_Id']],dataProp_dct['Relationship']:vars()[dataProp_dct['Relationship']],dataProp_dct['Account_Id']:account_details[0],dataProp_dct['Account_Status']:account_details[1],dataProp_dct['Account_Open_Date']:account_details[2],dataProp_dct['Account_Closed_Date']:account_details[3],dataProp_dct['Account_Balance']:account_details[4],dataProp_dct['Extension']:vars()[dataProp_dct['Extension']],dataProp_dct['Home_Phone_Number']:vars()[dataProp_dct['Home_Phone_Number']],dataProp_dct['Mobile_Phone_Number']:vars()[dataProp_dct['Mobile_Phone_Number']],dataProp_dct['Work_Phone_Number']:vars()[dataProp_dct['Work_Phone_Number']],dataProp_dct['Address_Type']:vars()[dataProp_dct['Address_Type']],dataProp_dct['Address_Line1']:vars()[dataProp_dct['Address_Line1']],dataProp_dct['Locality']:vars()[dataProp_dct['Locality']],dataProp_dct['Language']:vars()[dataProp_dct['Language']]}
        elif super_classes_leaves[classes[1]][1]==person:
            return {dataProp_dct['Title']:vars()[dataProp_dct['Title']],dataProp_dct['First_Name']:vars()[dataProp_dct['First_Name']],dataProp_dct['Middle_Name']:vars()[dataProp_dct['Middle_Name']],dataProp_dct['Last_Name']:vars()[dataProp_dct['Last_Name']],dataProp_dct['Email_Address']: vars()[dataProp_dct['Email_Address']],dataProp_dct['Gender']:vars()[dataProp_dct['Gender']],dataProp_dct['Date_of_Birth']:vars()[dataProp_dct['Date_of_Birth']],dataProp_dct['Company']:emp_details[0],dataProp_dct['Sector']:emp_details[1],dataProp_dct['Designation']:emp_details[2],dataProp_dct['From_Date']:emp_details[3],dataProp_dct['To_Date']:emp_details[4],dataProp_dct['Account_Id']:account_details[0],dataProp_dct['Account_Status']:account_details[1],dataProp_dct['Account_Open_Date']:account_details[2],dataProp_dct['Account_Closed_Date']:account_details[3],dataProp_dct['Account_Balance']:account_details[4],dataProp_dct['Extension']:vars()[dataProp_dct['Extension']],dataProp_dct['Home_Phone_Number']:vars()[dataProp_dct['Home_Phone_Number']],dataProp_dct['Mobile_Phone_Number']:vars()[dataProp_dct['Mobile_Phone_Number']],dataProp_dct['Work_Phone_Number']:vars()[dataProp_dct['Work_Phone_Number']],dataProp_dct['Address_Type']:vars()[dataProp_dct['Address_Type']],dataProp_dct['Address_Line1']:vars()[dataProp_dct['Address_Line1']],dataProp_dct['Locality']:vars()[dataProp_dct['Locality']],dataProp_dct['Language']:vars()[dataProp_dct['Language']]}
        else:
            return {dataProp_dct['Title']:vars()[dataProp_dct['Title']],dataProp_dct['First_Name']:vars()[dataProp_dct['First_Name']],dataProp_dct['Middle_Name']:vars()[dataProp_dct['Middle_Name']],dataProp_dct['Last_Name']:vars()[dataProp_dct['Last_Name']],dataProp_dct['Email_Address']: vars()[dataProp_dct['Email_Address']],dataProp_dct['Gender']:vars()[dataProp_dct['Gender']],dataProp_dct['Date_of_Birth']:vars()[dataProp_dct['Date_of_Birth']],dataProp_dct['Company']:emp_details[0],dataProp_dct['Sector']:emp_details[1],dataProp_dct['Designation']:emp_details[2],dataProp_dct['From_Date']:emp_details[3],dataProp_dct['To_Date']:emp_details[4],dataProp_dct['Account_Id']:account_details[0],dataProp_dct['Account_Status']:account_details[1],dataProp_dct['Account_Open_Date']:account_details[2],dataProp_dct['Account_Closed_Date']:account_details[3],dataProp_dct['Account_Balance']:account_details[4],dataProp_dct['Extension']:vars()[dataProp_dct['Extension']],dataProp_dct['Home_Phone_Number']:vars()[dataProp_dct['Home_Phone_Number']],dataProp_dct['Mobile_Phone_Number']:vars()[dataProp_dct['Mobile_Phone_Number']],dataProp_dct['Work_Phone_Number']:vars()[dataProp_dct['Work_Phone_Number']],dataProp_dct['Address_Type']:vars()[dataProp_dct['Address_Type']],dataProp_dct['Address_Line1']:vars()[dataProp_dct['Address_Line1']],dataProp_dct['Locality']:vars()[dataProp_dct['Locality']],dataProp_dct['Language']:vars()[dataProp_dct['Language']]}


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
        col=[list(d[k](self.dataProp_dct,self.classes,self.ranges,self.companies_lst,self.isd,self.data_prop,self.type_of_person,self.type_of_account,self.super_classes_leaves).keys()) for k in d.keys()][0]

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
        