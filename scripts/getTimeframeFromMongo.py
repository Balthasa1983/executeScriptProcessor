import sys, traceback, time, re, os, base64
from org.apache.nifi.processor.io import OutputStreamCallback
from org.python.core.util import StringUtil
from pymongo import MongoClient

class WriteContentCallback(OutputStreamCallback):
    def __init__(self, content):
        self.content_text = content

    def process(self, outputStream):
        try:
            outputStream.write(StringUtil.toBytes(self.content_text))
        except:
            traceback.print_exc(file=sys.stdout)
            raise

def makeSureFileExist(file):
    try:
        open(file, 'r')
        print 'filelist already exist'
    except IOError:
        open(file, 'w')
        print 'filelist was successfully created'

alreadytransfered = []

timeframe = fromDate.getValue() + "to" + toDate.getValue()

ftype = filetype.getValue()

gtranachive = archive.getValue()

env = enviroment.getValue()

wFile = withFile.getValue()

#fileinformation to handle already processed files
fileroot = "/u11/tmp/bots/filehandling/"
filename = env + timeframe + ftype + gtranachive + ".txt"
filepath = fileroot + filename

#configuration mongodb client and query
client = MongoClient('mongodb://mongodbenv:27017')
db = client['Interfacefiles']
collection = db[gtranachive]
result = collection.find({'$and': [ { "day": { '$gte': fromDate.getValue() } }, { "day": { '$lte': toDate.getValue() } } ], "filetype": ftype})
#read already processed file to already processed list
makeSureFileExist(filepath)
oldfiles = open(filepath, "r")
alreadytransfered = oldfiles.read().splitlines()
oldfiles.close()

#iterate over files in query result and create flowfileobjects for not already processed files
if wFile == "yes":
    for file in result:
        oldfiles = open(filepath, "a")
        oldfiles.write(str(file).split("u'filename':")[1] + "\n")
        oldfiles.close()

        filecontent = str(file)

        try:
            # extract filetype
            searchstring = r"(?<=u\'filetype\'\: u\')(.*?)(?=\')"
            cleanfiletype = re.search(searchstring, filecontent).group()
        except:
            cleanfiletype = str(sys.exc_info()[0])

        try:
            # extract adn clean Day
            searchstring = r"(?<=u\'day\'\: u\')(.*?)(?=\')"
            cleanday = re.search(searchstring, filecontent).group()
        except:
            cleanday = str(sys.exc_info()[0])

        try:
            # extract and clean filename
            searchstring = r"(?<=u\'filename\'\: u\')(.*?)(?=\')"
            cleanfilename = re.search(searchstring, filecontent).group()
        except:
            cleanfilename = str(sys.exc_info()[0])

        try:
            # extract and clean archive
            searchstring = r"(?<=u\'archive\'\: u\')(.*?)(?=\')"
            cleanarchive = re.search(searchstring, filecontent).group()
        except:
            cleanarchive = str(sys.exc_info()[0])

        try:
            # exctract, decode and clean content
            searchstring = r"(?<=u\'content\'\: u\')(.*?)(?=\')"
            cleanedencodedcontent = re.search(searchstring, filecontent).group().replace('\\r\\n', '')
            #data = base64.b64decode(cleanedencodedcontent)
            #cleaneddecodedcontent = re.search('(?<=b\")(.*)(?=\")', str(data))
            #cleancontent = java.lang.String(str(cleaneddecodedcontent[0]), "utf-8")
            cleancontent = str(cleanedencodedcontent)
        except:
            cleancontent = str(sys.exc_info()[0])

        attrMap = {'archive': cleanarchive, 'filetype': cleanfiletype, 'day': cleanday,
                   'filename': cleanfilename}

        myflowFile = session.create()
        myflowFile = session.write(myflowFile, WriteContentCallback(cleancontent))
        myflowFile = session.putAllAttributes(myflowFile, attrMap)
        session.transfer(myflowFile, REL_SUCCESS)
else:
    for file in result:
        if str(file).split("u'filename':")[1] not in alreadytransfered:
            oldfiles = open(filepath, "a")
            oldfiles.write(str(file).split("u'filename':")[1] + "\n")
            oldfiles.close()

            filecontent = str(file)

            try:
                # extract filetype
                searchstring = r"(?<=u\'filetype\'\: u\')(.*?)(?=\')"
                cleanfiletype = re.search(searchstring, filecontent).group()
            except:
                cleanfiletype = str(sys.exc_info()[0])

            try:
                # extract adn clean Day
                searchstring = r"(?<=u\'day\'\: u\')(.*?)(?=\')"
                cleanday = re.search(searchstring, filecontent).group()
            except:
                cleanday = str(sys.exc_info()[0])

            try:
                # extract and clean filename
                searchstring = r"(?<=u\'filename\'\: u\')(.*?)(?=\')"
                cleanfilename = re.search(searchstring, filecontent).group()
            except:
                cleanfilename = str(sys.exc_info()[0])

            try:
                # extract and clean archive
                searchstring = r"(?<=u\'archive\'\: u\')(.*?)(?=\')"
                cleanarchive = re.search(searchstring, filecontent).group()
            except:
                cleanarchive = str(sys.exc_info()[0])

            try:
                # exctract, decode and clean content
                searchstring = r"(?<=u\'content\'\: u\')(.*?)(?=\')"
                cleanedencodedcontent = re.search(searchstring, filecontent).group().replace('\\r\\n', '')
                #data = base64.b64decode(cleanedencodedcontent)
                #cleaneddecodedcontent = re.search('(?<=b\")(.*)(?=\")', str(data))
                #cleancontent = java.lang.String(str(cleaneddecodedcontent[0]), "utf-8")
                cleancontent = str(cleanedencodedcontent)
            except:
                cleancontent = str(sys.exc_info()[0])

            attrMap = {'archive': cleanarchive, 'filetype': cleanfiletype, 'day': cleanday,
                       'filename': cleanfilename}

            myflowFile = session.create()
            myflowFile = session.write(myflowFile, WriteContentCallback(cleancontent))
            myflowFile = session.putAllAttributes(myflowFile, attrMap)
            session.transfer(myflowFile, REL_SUCCESS)
