

from pyspark import SparkContext

if __name__ == "__main__":

    sc       = SparkContext(appName="Test")
    files    = sc.wholeTextFiles('s3://ukpolice/police/')
    files3   = files.map(lambda objects: (objects[0]))
    filelist = files3.collect()

    for file in filelist:

        length       = len(file)
        streetstart  = length - 10
        sandsstart   = length - 19
        outstart     = length - 12
        end          = length - 4

        lines        = sc.textFile(file)
        firstline    = lines.first()

        ErrStreet    = 0
        ErrSandS     = 0
        ErrOutcomes  = 0
        
        if file[streetstart:end]=="street":
            if firstline!="Crime ID,Month,Reported by,Falls within," \
                          "Longitude,Latitude,Location,LSOA code,LSOA name," \
                          "Crime type,Last outcome category,Context":
                print("ERROR - Street")
                print(file)
                ErrStreet = ErrStreet + 1

        elif file[sandsstart:end]=="stop-and-search":
            if firstline!="Type,Date,Part of a policing operation," \
                          "Policing operation,Latitude,Longitude,Gender," \
                          "Age range,Self-defined ethnicity," \
                          "Officer-defined ethnicity,Legislation," \
                          "Object of search,Outcome,Outcome linked to object of search," \
                          "Removal of more than just outer clothing":
                print("ERROR - SandS")
                print(file)
                ErrSandS = ErrSandS + 1

        elif file[outstart:end]=="outcomes":
            if firstline!="Crime ID,Month,Reported by,Falls within," \
                          "Longitude,Latitude,Location,LSOA code,LSOA name," \
                          "Outcome type":
                print("ERROR - Outcomes")
                print(file)
                ErrOutcomes = ErrOutcomes + 1

        else:
            print("ERROR - Unidentified")
            print(file)
print("Total Street Errors:")
print(ErrStreet)
print("Total SandS Errors:")
print(ErrSandS)
print("Total Outcomes Errors:")
print(ErrOutcomes)
print("DONE!")
