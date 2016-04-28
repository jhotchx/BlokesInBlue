
from numpy import array

from pyspark.mllib.regression import LinearRegressionWithSGD 
test = sc.textFile('s3://ukpolice/NOMIS/unempLADtest.csv')
test1 =  test.map(lambda line: line.split(',')).filter(lambda line: (line[0]!='!') and (line[0]!='-') and (line[1]!='!') and (line[0]!='~') and (line[0]!='#') and (line[0]!='*')) 

data = test1.map(parsePoint)
labeled_points_rdd = test1.map(lambda seq: LabeledPoint(seq[0],seq[1:]))
list1 = labeled_points_rdd.collect()[0:]

lrm = LinearRegressionWithSGD.train(list1)

# get rid of header row
#test1 = test.collect()[1:]
# back to rdd
#test2 = sc.parallelize(test1)
