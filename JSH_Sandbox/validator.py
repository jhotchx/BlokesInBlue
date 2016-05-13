
from pyspark import Accumulator
from pyspark import SparkContext

if __name__ == "__main__":

    sc        = SparkContext(appName="Test")
    files     = sc.wholeTextFiles('s3://ukpolice/police/2015-12/')
    justnames = files.map(lambda objects: (objects[0]))
    filelist  = justnames.collect()

    for file in filelist:

        length       = len(file)
        streetstart  = length - 10
        sandsstart   = length - 19
        outstart     = length - 12
        end          = length - 4

        lines        = sc.textFile(file)
        firstline    = lines.first()

        if file[streetstart:end]=="street":
            print("street")
            if firstline!="Crime ID,Month,Reported by,Falls within," \
                          "Longitude,Latitude,Location,LSOA code,LSOA name," \
                          "Crime type,Last outcome category,Context":
                print("ERROR - Street")
                print(file)

        elif file[sandsstart:end]=="stop-and-search":
            print("SandS")
            if firstline!="Type,Date,Part of a policing operation," \
                          "Policing operation,Latitude,Longitude,Gender," \
                          "Age range,Self-defined ethnicity," \
                          "Officer-defined ethnicity,Legislation," \
                          "Object of search,Outcome,Outcome linked to object of search," \
                          "Removal of more than just outer clothing":
                print("ERROR - SandS")
                print(file)

        elif file[outstart:end]=="outcomes":
            NumOutcomes += 1
            if firstline!="Crime ID,Month,Reported by,Falls within," \
                          "Longitude,Latitude,Location,LSOA code,LSOA name," \
                          "Outcome type":
                print("ERROR - Outcomes")
                print(file)

        else:
            print("ERROR - Unidentified")
            print(file)

    print("DONE!")
