import string
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("appName").setMaster("local")
sc = SparkContext(conf=conf)

myfile = sc.textFile("hdfs://localhost:9000/home/hadoop/Input/")
lemma = sc.textFile("hdfs://localhost:9000/home/hadoop/Lemma/")
lemmaList={}

def word_pairs(uniLine):
	lines=uniLine.encode('utf-8')
	pos=lines.find(">")
	loc=lines[:pos+1]
	line = lines[pos+1:]
	#fullLine=lines.split(">")
	#loc=fullLine[0]+">"
	#line = fullLine[1]
	line = line.translate(None, string.punctuation)
	line = line.lower()
	line=line.replace("j","i")
	line=line.replace("v","u")
	words = line.split()	
	l=[]
	count = 1
	for i in  range(0,len(words)):
		for n in range(i+1, len(words)):
			if words[i] in lemmaList and words[n] in lemmaList:
				val1 = lemmaList[words[i]]
				val2 = lemmaList[words[n]]
				for v1 in val1:
					for v2 in val2:
						temp = v1+" - "+v2+"$"+`count`
						l.append(temp)
			else:
				temp= words[i]+" - "+words[n]+"$"+`count`
				l.append(temp)				
		count=count+1	
	l.append(loc)
	return l

def lemma_map(lines):
	for line in lines:
		words = line.split(",")
		count = 0
		l=[]
		count=0
		for n in range(1, len(words)):
			if len(words[n])>0:
				l.append(words[n])
		lemmaList[words[0]]=l
	return l

def locationSplit(wordLineList):	
	print len(wordLineList)
	last=wordLineList.pop()
	print last
	print len(wordLineList)

	return ((wordLineList[i].split("$")[0],last.replace(">",". "+wordLineList[i].split("$")[1]+">")) for i in range(0,len(wordLineList)))

m=lemma_map(lemma.collect())

fileLines=myfile.collect()
fileLines=filter(None,fileLines)

pairs1= sc.parallelize(fileLines).map(word_pairs).flatMap(locationSplit).reduceByKey(lambda a, b: a + b)
pairs1.saveAsTextFile("hdfs://localhost:9000/home/hadoop/Output2/")
#for x in pairs1.collect():
#	y=x

