
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

        ErrStreet       = 0
        ErrSandS        = 0
        ErrOutcomes     = 0
        ErrUnidentified = 0
        
        NumStreet       = sc.accumulator(0)
        NumSandS        = sc.accumulator(0)
        NumOutcomes     = sc.accumulator(0)

        if file[streetstart:end]=="street":
            print("street")
            NumStreet += 1
            if firstline!="Crime ID,Month,Reported by,Falls within," \
                          "Longitude,Latitude,Location,LSOA code,LSOA name," \
                          "Crime type,Last outcome category,Context":
                print("ERROR - Street")
                print(file)
                ErrStreet = ErrStreet + 1

        elif file[sandsstart:end]=="stop-and-search":
            print("SandS")
            NumSandS += 1
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
            NumOutcomes += 1
            if firstline!="Crime ID,Month,Reported by,Falls within," \
                          "Longitude,Latitude,Location,LSOA code,LSOA name," \
                          "Outcome type":
                print("ERROR - Outcomes")
                print(file)
                ErrOutcomes = ErrOutcomes + 1

        else:
            print("ERROR - Unidentified")
            print(file)
            ErrUnidentified = ErrUnidentified + 1

    print("Total Street Files:")
    print(NumStreet.value)
    print("Total Street Errors:")
    print(ErrStreet)
    print("Total SandS Files:")
    print(NumSandS.value)
    print("Total SandS Errors:")
    print(ErrSandS)
    print("Total Outcomes Files:")
    print(NumOutcomes.value)
    print("Total Outcomes Errors:")
    print(ErrOutcomes)
    print("Total Unidentified Errors:")
    print(ErrUnidentified)
    print("DONE!")
