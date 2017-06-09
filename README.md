# Word Coocurrence on Latin files using MapReduce and Apache Spark(pyspark) 

**Files:**
1. /WoocPairsLatin.java : MapReduce program to obtain the word coocurrence with lemmatization.
2. /2gram.py : Python program using pyspark module to obtaing the word coocurrence with lemmatization.

NOTE: For some reasons the files can't be provided so following is the single line format of input and output.

**Input:**
There were two folders 1. input and 2. csv

input folder contained multiple Latin files each having the following structure:
<author_id, doc_id, chap no.line no> Text

csv folder contained a csv file (for lemmatization) having the first column as the word to be lemmatized and the remaining columns representing it's lemmas for each row.

**Output of MapReduce:**
Format: word1,word2 <author_id. doc_id. chap_no.line_no. position>
Example: tantus,longinquus	<verg. aen. 3.415. 1>'

**Output of Spark:**
Format: (u'word1 - word2', u'<author_id. doc_id. chap_no.line_no. position>')
Example: (u'tantus - longinquus', u'<verg. aen. 3.415. 1>')