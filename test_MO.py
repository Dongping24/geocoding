import numpy as np
import csv
import xlrd
import pandas as pd
import ngram
import os
import Levenshtein
from multiprocessing import Pool
import ast
import glob
import time
import easygui as eg
import pp
import os
import sys
sys.path.extend(['./../../','./../GeoCodes'])
sys.dont_write_bytecode = True

from GeoCodes import *
from functions import *
import csv_handler

# Input Path:
path = '/Users/Dongping/Dropbox/files/dissertation/data/bankruptcy/spreadsheets/missouri-st.louis/raw/'
filelist = os.listdir(path)
filelist.remove('.DS_Store') # remove unnecessary files; this is for mac; make accordin change if use PC

aaddress = [] # attorney's address
acity = [] # attorney's city
astate = [] # attorney' state
azip = [] # attorney's zip code
acounty = [] # attorney's county

# Loop through each file in the directionary:
for xls in filelist:
    print "Reading", xls
    file_loc = path + xls
    workbook = xlrd.open_workbook(file_loc) # open up the workbook
    sheet = workbook.sheet_by_name("People and Places") # open up the sheet

    for rowx in range(2,sheet.nrows): # ignore the first 2 rows: 1st - empty; 2nd - variable name
        address = sheet.cell_value(rowx,9) # Column J (or index 9 in python) - street address
        city = sheet.cell_value(rowx,10) # Column K - city
        state = sheet.cell_value(rowx,11) # Column L - state
        zipcode = sheet.cell_value(rowx,12) # Column M - zip code

        if type(address) == float:
            aaddress.append(int(address)) # "-99.0" -> "99"
        else:
            address = address.encode('ascii','ignore') # unicode -> string
            if address == '':
                aaddress.append(-9999) # blank -> -9999
            else:
                aaddress.append(address)


        if type(city) == float:
            acity.append(int(city)) # "-99.0" -> "99"
        else:
            city = city.encode('ascii','ignore') # unicode -> string
            acity.append(city)

        if type(state) == float:
            astate.append(int(state)) # "-99.0" -> "99"
        else:
            state = state.encode('ascii','ignore') # unicode -> string
            astate.append(state)

        if type(zipcode) == float:
            azip.append(int(zipcode)) # "-99.0" -> "99"
        else:
            zipcode = zipcode.encode('ascii','ignore') # unicode -> string
            azip.append(zipcode)

        acounty.append('')

# assert all variables are written for each obs:
assert len(aaddress) == len(acity) == len(astate) == len(azip) == len(acounty)

# write to csv:
with open('./in/MO_Attorney.csv','wb') as fp:
    csvout = csv.writer(fp)
    csvout.writerows(zip(aaddress,acity,astate,azip,acounty))



x = ['../CLEAN','../TEMP','../TEMPFILES']
for i in x:
    cleanfolder(i)
directory = './in'
#directory = eg.diropenbox('SELECT INPUT FOLDER')
filelist = glob.glob(directory+'/*.csv')
directory1 = './out'
#directory1 = eg.diropenbox('SELECT OUTPUT FOLDER')
outputdirec = directory1
maxprocess = 1
#maxprocess = eg.enterbox(msg="ENTER MAXIMUM NUMBER OF PROCESSES", title="PROCESS QUERY")
if maxprocess == None:
    print'Execution Canceled'
    sys.exit()
else:
    maxprocess = int(maxprocess)
countrycodedic = {}
countrycode = csv_handler.readcsv('../INPUT/countrycode.csv')
for x in countrycode:
    if x[0].strip()[0] != '#':
        cls = x[0].strip().split(';')
        for i in range(len(cls)):
            countrycodedic[cls[i].strip().upper()] = cls[0].strip().upper()
totalprocess = len(filelist)
if totalprocess < maxprocess:
    filelistout = filelist
    totalprocess = totalprocess
    tempfilelist, dict_of_address_to_file = phase1(filelist,countrycodedic) #country check    """
    tempfilelist = phase2(tempfilelist) #state check
    finalcheckfile = phase3(tempfilelist) #cleanining
    maxLevenshtein = 2 #set Maximum level for Leveshtein match
    
    ls = []
    for each in finalcheckfile:
        ls.append(csv_handler.readcsv(each))
    lslen = len(ls)
    totalprocess = lslen
    pass #pass #print"Code running for "+str(totalprocess)+" no. of files
    jobs = []
    job_server = pp.Server(secret="abc")
    job_server.set_ncpus(int(totalprocess))
    # pool = Pool(processes=totalprocess)
    for i in range(totalprocess):
        lsvalidlocs = ls[i]
        jobs.append(job_server.submit(mainprog, (lsvalidlocs,countrycodedic,i,maxLevenshtein)))
        # mainprog(lsvalidlocs,countrycodedic,i,maxLevenshtein)
        # pool.apply_async(mainprog, args = (lsvalidlocs,countrycodedic,i,maxLevenshtein, ))
    # pool.close()
    # pool.join()
    for job in jobs:
        print job()
    ls = None #Releasing memory

    dirc = '../TEMPFILES'
    listfiles = os.listdir(dirc)
    count = 0
    for x in listfiles:
        recordlist = []
        ls = []
        ls = csv_handler.readcsv(dirc+"/"+x)
        for y in ls:
            recordlist.append(y)
        recordlist = cleanrecords(recordlist,maxLevenshtein)
        pass #pass #printlen(recordlist)
        csv_handler.appendcsv('../CLEAN/OUTPUT'+str(count)+'.csv',recordlist)
        os.remove(dirc+"/"+x)
        ls = None #Releasing memory
        count += 1
    
    dirc = outputdirec
    listfiles = os.listdir(dirc)
    count = 0
    dirc = '../CLEAN'
    listfiles = os.listdir(dirc)
    for x in listfiles:
        recordlist = [['ADDRESS','CITY','STATE','COUNTRY','ZIP','COUNTY','','ADDRESS','COUNTRY ID','COUNTRY','STATE ID','STATE','COUNTY ID','COUNTY','CITY ID','CITY','ZIP','HAVE ALTERNATE COUNTY','ALTERNATE COUNTY NAME','ALTERNATE COUNTY ID','CITY FIPS CODE']]
        ls = []
        ls = csv_handler.readcsv(dirc+"/"+x)
        addresslist_to_append = dict_of_address_to_file[str(filelistout[count][filelistout[count].rindex('/')+1:])]
        counttemp165 = 0
        for y in ls:
            y.insert(0,addresslist_to_append[counttemp165])
            y.insert(7,addresslist_to_append[counttemp165])
            recordlist.append(y)
            # pass #printrecordlist[counttemp165]
            counttemp165 += 1
        # pass #printstr(recordlist)
        csv_handler.appendcsv(outputdirec+'/OUTPUT'+str(filelistout[count][filelistout[count].rindex('/')+1:]),recordlist)
        os.remove(dirc+"/"+x)
        ls = None #Releasing memory
        count += 1
    dirc = '../TEMP'
    listfiles = os.listdir(dirc)
    for x in listfiles:
        os.remove(dirc+"/"+x)
elif totalprocess == maxprocess:
    filelistout = filelist
    totalprocess = maxprocess
    tempfilelist, dict_of_address_to_file = phase1(filelist,countrycodedic) #country check    """
    tempfilelist = phase2(tempfilelist) #state check
    finalcheckfile = phase3(tempfilelist) #cleanining
    maxLevenshtein = 2 #set Maximum level for Leveshtein match
    ls = []
    for each in finalcheckfile:
        ls.append(csv_handler.readcsv(each))
    lslen = len(ls)
    totalprocess = lslen
    pass #pass #print"Code running for "+str(totalprocess)+" no. of files"
    # pool = Pool(processes=totalprocess)
    jobs = []
    job_server = pp.Server(secret="abc")
    job_server.set_ncpus(int(totalprocess))
    for i in range(totalprocess):
        lsvalidlocs = ls[i]
        jobs.append(job_server.submit(mainprog, (lsvalidlocs,countrycodedic,i,maxLevenshtein)))
        # pool.apply_async(mainprog, args = (lsvalidlocs,countrycodedic,i,maxLevenshtein, ))
    # pool.close()
    # pool.join()
    for job in jobs:
        print job()
    ls = None #Releasing memory

    dirc = '../TEMPFILES'
    listfiles = os.listdir(dirc)
    count = 0
    for x in listfiles:
        recordlist = []
        ls = []
        ls = csv_handler.readcsv(dirc+"/"+x)
        for y in ls:
            recordlist.append(y)
        recordlist = cleanrecords(recordlist,maxLevenshtein)
        pass #pass #printlen(recordlist)
        csv_handler.appendcsv('../CLEAN/OUTPUT'+str(count)+'.csv',recordlist)
        os.remove(dirc+"/"+x)
        ls = None #Releasing memory
        count += 1
    
    dirc = outputdirec
    listfiles = os.listdir(dirc)
    count = 0
    dirc = '../CLEAN'
    listfiles = os.listdir(dirc)
    for x in listfiles:
        recordlist = [['ADDRESS','CITY','STATE','COUNTRY','ZIP','COUNTY','','ADDRESS','COUNTRY ID','COUNTRY','STATE ID','STATE','COUNTY ID','COUNTY','CITY ID','CITY','ZIP','HAVE ALTERNATE COUNTY','ALTERNATE COUNTY NAME','ALTERNATE COUNTY ID','CITY FIPS CODE']]
        ls = []
        ls = csv_handler.readcsv(dirc+"/"+x)
        addresslist_to_append = dict_of_address_to_file[str(filelistout[count][filelistout[count].rindex('/')+1:])]
        counttemp165 = 0
        for y in ls:
            y.insert(0,addresslist_to_append[counttemp165])
            y.insert(7,addresslist_to_append[counttemp165])
            recordlist.append(y)
            # pass #printrecordlist[counttemp165]
            counttemp165 += 1
        # pass #printstr(recordlist)
        csv_handler.appendcsv(outputdirec+'/OUTPUT'+str(filelistout[count][filelistout[count].rindex('/')+1:]),recordlist)
        os.remove(dirc+"/"+x)
        ls = None #Releasing memory
        count += 1
    dirc = '../TEMP'
    listfiles = os.listdir(dirc)
    for x in listfiles:
        os.remove(dirc+"/"+x)

elif totalprocess > maxprocess:
    remtm = totalprocess%maxprocess
    if remtm == 0:
        totalloops = int(totalprocess/maxprocess)
    elif remtm > 0:
        totalloops = int(totalprocess/maxprocess)+1
    for i in range(0,totalloops):
        app = filelist[i*maxprocess:(i+1)*maxprocess]
        filelistout = app
        #pass #printapp
        tempfilelist, dict_of_address_to_file = phase1(app,countrycodedic) #country check    """
        tempfilelist = phase2(tempfilelist) #state check
        finalcheckfile = phase3(tempfilelist) #cleanining
        maxLevenshtein = 2 #set Maximum level for Leveshtein match
        ls = []
        for each in finalcheckfile:
            ls.append(csv_handler.readcsv(each))
        lslen = len(ls)
        totalprocess = lslen
        pass #pass #print"Code running for "+str(totalprocess)+" no. of files"
        jobs = []
        job_server = pp.Server(secret="abc")
        job_server.set_ncpus(int(totalprocess))

        # pool = Pool(processes=totalprocess)
        for i in range(totalprocess):
            lsvalidlocs = ls[i]
            jobs.append(job_server.submit(mainprog, (lsvalidlocs,countrycodedic,i,maxLevenshtein)))
            # pool.apply_async(mainprog, args = (lsvalidlocs,countrycodedic,i,maxLevenshtein, ))
        # pool.close()
        # pool.join()
        for job in jobs:
            print job()
        ls = None #Releasing memory

        dirc = '../TEMPFILES'
        listfiles = os.listdir(dirc)
        count = 0
        for x in listfiles:
            recordlist = []
            ls = []
            ls = csv_handler.readcsv(dirc+"/"+x)
            for y in ls:
                recordlist.append(y)
            recordlist = cleanrecords(recordlist,maxLevenshtein)
            csv_handler.appendcsv('../CLEAN/OUTPUT'+str(count)+'.csv',recordlist)
            os.remove(dirc+"/"+x)
            ls = None #Releasing memory
            count += 1
        
        dirc = outputdirec
        listfiles = os.listdir(dirc)
        count = 0
        dirc = '../CLEAN'
        listfiles = os.listdir(dirc)
        for x in listfiles:
            recordlist = [['ADDRESS','CITY','STATE','COUNTRY','ZIP','COUNTY','','ADDRESS','COUNTRY ID','COUNTRY','STATE ID','STATE','COUNTY ID','COUNTY','CITY ID','CITY','ZIP','HAVE ALTERNATE COUNTY','ALTERNATE COUNTY NAME','ALTERNATE COUNTY ID','CITY FIPS CODE']]
            ls = []
            ls = csv_handler.readcsv(dirc+"/"+x)
            addresslist_to_append = dict_of_address_to_file[str(filelistout[count][filelistout[count].rindex('/')+1:])]
            counttemp165 = 0
            for y in ls:
                y.insert(0,addresslist_to_append[counttemp165])
                y.insert(7,addresslist_to_append[counttemp165])
                recordlist.append(y)
                # pass #printrecordlist[counttemp165]
                counttemp165 += 1
            # pass #printstr(recordlist)
            csv_handler.appendcsv(outputdirec+'/OUTPUT'+str(filelistout[count][filelistout[count].rindex('/')+1:]),recordlist)
            os.remove(dirc+"/"+x)
            ls = None # Releasing memory
            count += 1
        dirc = '../TEMP'
        listfiles = os.listdir(dirc)
        for x in listfiles:
            os.remove(dirc+"/"+x)
