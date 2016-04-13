
from pyspark import SparkContext

if __name__ == "__main__":

    sc       = SparkContext(appName="Test")
    files    = sc.wholeTextFiles('s3://ukpolice/police/2015-10/')
    files3   = files.map(lambda objects: (objects[0]))
    filelist = files3.collect()

    streetfiles  =[]
    outcomesfiles=[]
    sandsfiles   =[]
    for file in filelist:

        length       = len(file)
        streetstart  = length - 10
        sandsstart   = length - 19
        outstart     = length - 12
        end          = length - 4

        if file[streetstart:end]=="street":
            streetfiles.append(file)

        elif file[sandsstart:end]=="stop-and-search":
            sandsfiles.append(file)

        elif file[outstart:end]=="outcomes":
            outcomesfiles.append(file)

        else:
            print("ERROR - Unidentified")
            print(file)

print("DONE!")
