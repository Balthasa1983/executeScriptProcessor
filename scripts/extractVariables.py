import re

def getFiletype(x):
    if "EDI" in x:
        return "EDI"
    elif "INH" in x:
        return "INH"
    else:
        return "unknown"


def getarchive(x):
    if "HAM" in x:
        return "HAM"
    elif "SYD" in x:
        return "SYD"
    elif "HKG" in x:
        return "HKG"
    else:
        return "unknown"


flowFile = session.get()

if (flowFile):
    try:
        path = flowFile.getAttribute("absolute.path")

        archive = getarchive(path)

        filetype = getFiletype(path)

        day = re.search('\d{8}', path).group(0)

        attrMap = {'archive': archive, 'filetype': filetype, 'day': day}

        flowFile = session.putAllAttributes(flowFile, attrMap)

        session.transfer(flowFile, REL_SUCCESS)
    except AttributeError:
        session.transfer(flowFile, REL_FAILURE)