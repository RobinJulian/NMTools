#!/bin/env python

import operator
from stat import ST_MTIME
import os, sys, time
import gzip
import re
from datetime import datetime
import logging
import argparse

numberFields = ['Subscription-Id-Data', 'Calling-Party-Address', 'Called-Party-Address', 'Requested-Party-Address', 'Related-Party-Address', 'Associated-Party-Address', 'User-Name', 'From-Address', 'Instance-Id', 'Called-Asserted-Identity']

def timestamp():
    now = datetime.now()
    return(now.strftime("%Y-%m-%d-%H:%M:%S"))

def anonymise(avp):
    avp = re.sub(r"three", "operator", avp)
    avp = re.sub(r"THREE", "OPERATOR", avp)
    avp = re.sub(r"Original-Called-Address", "Requested-Party-Address", avp)
    for i in range(len(numberFields)):
        if(avp.startswith("F " + numberFields[i])):
            listAVP = list(avp)
            listAVP[:] = [str((int(x)+5)%10) if(x.isdigit()) else x for x in listAVP]
            avp = "".join(listAVP)
    avp = re.sub(r"mnc...", "mnc123", avp)
    avp = re.sub(r"232F402", "232F492", avp)
    return(avp)

def writeRecordsToFile(filePath, capturedRecord):
    filename = "/tmp/capture-" + os.path.basename(filePath) + "-" + timestamp() + ".txt"
    filename = re.sub(r".dat.gz", "", filename)
    print("Writing file " + filename)
    outFile = open(filename, "w")

    for line in capturedRecord:
        if(line.startswith("F")):
            line = anonymise(line)
        outFile.write(line)

    logging.debug("Closing file")
    outFile.close()

def searchGzFile(filePath, searchPattern):
    logging.debug("searching file for pattern " + searchPattern + " in file " + filePath)

    matchInRecordFlag = False;
    matchingRecords = []
    capturedRecord = []

    inFile=gzip.open(filePath,'rt')

    try:
        for line in inFile:
            capturedRecord.append(line)
            if(re.search("^RECORD", line)):
                logging.debug("Found a RECORD start - matchInRecordFlag is " + str(matchInRecordFlag))
                if(matchInRecordFlag):
                    capturedRecord.pop(-1) # Remove RECORD from end
                    matchingRecords.extend(capturedRecord)
                    logging.debug("Record written setting matchInRecordFlag to False")
                capturedRecord=[]
                capturedRecord.append(line) # Ensure next record begins with RECORD
                matchInRecordFlag = False

            if(re.search(searchPattern, line)):
                logging.debug("Found match in file " + filePath + " setting matchInRecordFlag to True")
                matchInRecordFlag = True;
    except StopIteration:
        logging.debug("Should never reach here!")
    else:
        logging.debug("Checking end of file for matching record")
        if(matchInRecordFlag):
            matchingRecords.extend(capturedRecord)
            logging.debug("End of file and last record matches")

    if(len(matchingRecords) > 0):
        logging.debug("Writing matching records from file " + filePath)
        writeRecordsToFile(filePath, matchingRecords)

def getNewFiles(dirPath, lastFileMtime):
    allFiles = os.listdir(dirPath);
    fileMtime = dict();
    for file in allFiles:
        try:
            fileMtime[file] = os.stat(file).st_mtime;
        except:
            logging.debug("File no longer exists " + file)

    sortedFiles = sorted(fileMtime.items(), key=operator.itemgetter(1), reverse=True)
    logging.debug(sortedFiles)
    if(len(sortedFiles) > 0):
        newFileMtime = sortedFiles[0][1]
    else:
        newFileMtime = lastFileMtime

    newFilesList = []
    for i in range(len(sortedFiles)):
        if(sortedFiles[i][1] > lastFileMtime):
            newFilesList.append(sortedFiles[i][0])
        else:
            break

    return(newFilesList, newFileMtime)

def waitForDirUpdate(dirPath, dirMtime):
    newDirMtime = 0
    while(True):
        newDirMtime = os.stat(dirPath).st_mtime
        #logging.debug ("new " + str(newDirMtime) + " old " + str(dirMtime))
        if(newDirMtime > dirMtime):
            logging.debug ("updating dirMtime")
            dirMtime = newDirMtime
            break
        time.sleep(0.1)
    return(newDirMtime)

parser = argparse.ArgumentParser(description='Capture and anonymise records from backup storage in gzipped Comptel export format.')
parser.add_argument('--debug', action='store_true', default=False, required=False, help='Enable debug logging')
parser.add_argument(dest='path', help='Directory where files are collected')
parser.add_argument(dest='searchPattern', help='Records with a matching value for this searchPattern will be extracted')
args = parser.parse_args()

if (args.debug):
    logging.basicConfig(filename='capture.log', level=logging.DEBUG, format="%(filename)s:%(lineno)d %(message)s")
    logging.debug("========================================== Starting ======================================")

if(os.path.isdir(args.path) is False):
    logging.debug(args.path + " is not a directory")
    exit(2)

path = str(args.path) + '/' ;
os.chdir(path)
searchPattern=args.searchPattern

dirMtime = time.time()
lastFileMtime = dirMtime
while(True):
    dirMtime = waitForDirUpdate(path, dirMtime)
    newFiles,lastFileMtime = getNewFiles(path, lastFileMtime)

    logging.debug ("New files")
    logging.debug (newFiles)
    logging.debug ("New mtime: " + str(lastFileMtime))
    for filename in newFiles:
        if (filename.endswith(".gz")):
            searchGzFile(filename, searchPattern)
        else:
            logging.debug("skipping file" + file)
