#!/usr/bin/python3

#TODO CHECK ALL Contains key map bindings, Install local database to insert the download progress to track after connection timeout;

import dropbox
import argparse
import six
import sys
import os
import time
import datetime
import unicodedata
from filehash import FileHash
import hashlib
import json 
import requests

#DROPBOX and Local Directory Structure
DROPBOX_DIRECTORY="/adServe"
LOCAL_DIR_PATH="/Users/vkatragadda/adServe"
DROPBOX_DOWNLOAD_URL="https://content.dropboxapi.com/2/files/download"
DROPBOX_TOKEN="######################"
DAYS_DELTA=2
HOURS_DELTA=2
MINUTES_DELTA=2
CURRENT_DOWNLOADED_FILE_SIZE=0
CURRENT_ITERATION_FILE_SIZE=0
DOWNLOAD_CHUNK_SIZE=5242880 #5mb
CURRENT_FILE_IDENTIFIER=""


#Exit the execution if there are no files to sync.
def checktocontinue(maplength):#TODO check how to move this function to helper.
    if(maplength == 0):
        sys.exit('::::No Inbound Files found to Sync:::::')

#Helper to convert mb to bytes
def convertmbtobytes(datainmb):
    return datainmb * 1048576

#kb to Mb
def convertbytestomb(datainkb):
    return datainkb/1048576

#Caller to Dropbox to get the file down by chunking.
def download_chunked_file(file_id,start_bytes,end_bytes,inboundfilename,file_size_to_download):
    payload = ""
    pathname = "{\"path\":\"" + file_id + "\"}"
    bytestr = "\"bytes="+str(start_bytes)+"-"+str(end_bytes)+"\""
    headers = {
    'Authorization': "Bearer "+DROPBOX_TOKEN,
    "Dropbox-API-Arg": pathname,
    'Range': bytestr
    }
    response = requests.request("POST", DROPBOX_DOWNLOAD_URL, data=payload, headers=headers,stream=False)
    localfilename = "download_in_progress"+"_"+inboundfilename
    file = open(localfilename, "w")
    file.write(response.text)
    file.close()
    download_file_size = os.path.getsize(LOCAL_DIR_PATH+"/"+localfilename)
    if download_file_size == file_size_to_download:
       print("Naming file to : "+inboundfilename) 
       os.rename(localfilename,inboundfilename) 

dbx =dropbox.Dropbox(DROPBOX_TOKEN)
print("Account logged in : ",dbx.users_get_current_account().email)
print("Finding Files from Dropbox remote directory : "+DROPBOX_DIRECTORY)
print("Starting to poll for data .... ")

time.sleep(2)

datetimetochecklastmodifieddate = datetime.datetime.now() - datetime.timedelta(days = DAYS_DELTA,hours=HOURS_DELTA,minutes=MINUTES_DELTA)
print("Space left on dropbox Account ",dbx.users_get_space_usage())#publish an MQTT if the space is below the required thresold.
filenames = []
inboundfilemap={}
inboundfileid_map={}
for entry in dbx.files_list_folder(DROPBOX_DIRECTORY).entries:
    filesize = entry.size/1048576
    print("File Found ",entry.name+" Size of the file =>",filesize,"mb")
    if entry.server_modified >= datetimetochecklastmodifieddate:
        print("File Modified ",entry.server_modified)
        print("File Id ",entry.id)
        filenames.append(entry.name)
        inboundfilemap[entry.name] = filesize
        inboundfileid_map[entry.name] = entry.id


print("Total files found on remote Directory ==>",len(filenames))
print("Inbound Files Dictionary => ",len(inboundfilemap))
checktocontinue(len(inboundfilemap))



#Find and compare the files in the local directory to check if the files exists.
localdirectoryfiles = os.listdir(LOCAL_DIR_PATH)
print("localdirfiles ",localdirectoryfiles)

localfilenamesizemap={}
for localfile in localdirectoryfiles:
    print("File Name : ",localfile)
    filesizeinmb  = (os.path.getsize(LOCAL_DIR_PATH+"/"+localfile))/1048576
    localfilenamesizemap[localfile] = filesizeinmb

print('size of local files dictionary=> ',len(localfilenamesizemap))

"""Find and compare the files with local directory and remove the one's we dont need."""

itemstobringfromdropbox=[]
for k,v in inboundfilemap.items():
    print("Key: ",k+" Value: ",v)
    if k not in localfilenamesizemap:
        print("Not Found in local Dictionary: ",k)
        itemstobringfromdropbox.append(k)
print("Items to download from Dropbox=> ",itemstobringfromdropbox)

"""__main__ start to bring the files down"""

for itemtoget in itemstobringfromdropbox:
    if itemtoget in inboundfilemap:
        CURRENT_ITERATION_FILE_SIZE = inboundfilemap[itemtoget]
        while CURRENT_DOWNLOADED_FILE_SIZE < CURRENT_ITERATION_FILE_SIZE:
            print("STARTING DOWNLOAD=> ",CURRENT_DOWNLOADED_FILE_SIZE,"ENDING WITH => ",(CURRENT_DOWNLOADED_FILE_SIZE +DOWNLOAD_CHUNK_SIZE))
            start_bytes = CURRENT_DOWNLOADED_FILE_SIZE
            end_bytes = (CURRENT_DOWNLOADED_FILE_SIZE +DOWNLOAD_CHUNK_SIZE)
            download_chunked_file(inboundfileid_map[itemtoget],start_bytes,end_bytes,itemtoget,inboundfilemap[itemtoget])
            CURRENT_DOWNLOADED_FILE_SIZE = end_bytes #saving the progress of download.

print("Downloaded the complete file ")



