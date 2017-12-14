import sys, traceback, re, uuid
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

def extractMainInfo(filecontent):
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
        cleancontent = str(cleanedencodedcontent)
    except:
        cleancontent = str(sys.exc_info()[0])

    return cleanfiletype, cleanday, cleanfilename, cleanarchive, cleancontent

ftype = filetype.getValue()
gtranachive = archive.getValue()

#configuration mongodb client and query
client = MongoClient('mongodb://hamvw910:27017')
db = client['Interfacefiles']
collection = db[gtranachive]
result = collection.find({'$and': [ { "day": { '$gte': fromDate.getValue() } }, { "day": { '$lte': toDate.getValue() } } ], "filetype": ftype})

#iterate over files in query result and create flowfileobjects for not already processed files

for file in result:
        filecontent = str(file)

        cleanfiletype, cleanday, cleanfilename, cleanarchive, cleancontent = extractMainInfo(filecontent)

        attrMap = {
            'archive': cleanarchive,
            'filetype': cleanfiletype,
            'day': cleanday,
            'src.day' : cleanday,
            'filename': cleanfilename,
            'src.filename' : cleanfilename,
            'src.env' : env.getValue(),
            'src.uuid' : str(uuid.uuid1())
                   }

        myflowFile = session.create()
        myflowFile = session.write(myflowFile, WriteContentCallback(cleancontent))
        myflowFile = session.putAllAttributes(myflowFile, attrMap)
        session.transfer(myflowFile, REL_SUCCESS)
        session.commit()




