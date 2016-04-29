# sudo pip install pandas
# a lot of this stuff is no longer necessary
import numpy
from numpy import array
import pandas
from pyspark.mllib.regression import LinearRegressionWithSGD
from pyspark.mllib.regression import LabeledPoint
from scipy import stats
from scipy.stats import linregress

sc     = SparkContext( appName="Dedup Street" )
sqlCtx = SQLContext(sc)

unemployment = pandas.read_csv('s3://ukpolice/NOMIS/UnemploymentLAD.csv', usecols=('local authority: district / unitary (prior to April 2015)','Unemployment rate - aged 16-64','Date'))
crime = pandas.read_csv('s3://ukpolice/total_crime_2011_LAD.csv', usecols=('LAD_name','count'))
colnames = ['LAD_name','unemp','Date']
unemployment.columns=colnames
unemployment['unemp'] = pandas.to_numeric(unemployment['unemp'])


## need to merge unemployment and crime data on LAD name
data = pandas.merge(unemployment,crime,how='inner',on='LAD_name')

# get rid of header row - pandas seems to do this automatically
# data = data.ix[1:]
data = data.drop('LAD_name', 1)
data=data[data['Date']==2011]
data=data.drop('Date',1)
data=data.dropna()
data = data.as_matrix()

x = data[:,0]
y = data[:,1]

lrm = linregress(x,y)
#data = data[data['unemp'] != '-']
#data = data[data['unemp'] != '!']
#data = data[data['unemp'] != '~']
#data = data[data['unemp'] != '*']
#data = data[data['unemp'] != '#']




#data = sqlCtx.createDataFrame(data)

#labeled_points_rdd = data.map(lambda seq: LabeledPoint(seq[0],seq[1:]))
#data = labeled_points_rdd.collect()[0:]

lrm = LinearRegressionWithSGD.train(sc.parallelize(data), iterations=10, initialWeights=numpy.array([1.0]))


# back to rdd
#test2 = sc.parallelize(test1)
