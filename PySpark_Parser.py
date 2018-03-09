'''
The currrent codebase was created after group project collaboration with Orestis Roussos(Ms.C. Software Engineering)
2016, December
'''
import json
import os
import gzip
import sys
import re
import pprint
from nltk import sent_tokenize, word_tokenize, pos_tag, ne_chunk, tree
from bs4 import BeautifulSoup
from pyspark import SparkContext

warc_type_regex = re.compile(r'((WARC-Type:).*)')
warc_record_id_regex = re.compile(r'((WARC-Record-ID:) <.*>)')
html_regex = re.compile(r'<html\s*(((?!<html|<\/html>).)+)\s*<\/html>', re.DOTALL)

sc = SparkContext("local", "knowledge-acquisition")

def get_warc_info(file_name):
	with gzip.open(file_name,'rb') as f:
		warc_id = "Warc_id pending."
		warc_content = f.read()
	return (warc_id, warc_content)	

def casualTokenizing(text):
	sentences = sent_tokenize(text)
	sentences = filter(lambda sent: sent != "", sentences)
	tokens = word_tokenize(text)

	return sentences, tokens

def extractUniqueEntities(tokens):
	unique_entities = []
	tagged_entities = ne_chunk(tokens, binary=False)
	for entity in tagged_entities:
		if isinstance(entity, tree.Tree):

			if entity not in unique_entities:
				unique_entities.append(entity)
	return unique_entities


def filterTokens(tokens):
 	stop_words = set(stopwords.words("english"))
 	filtered_tokens = []
 	for t in tokens:
 		if t not in stop_words:
 			filtered_tokens.append(t)
 	return filtered_tokens

def validateInput(file_name):
	if "warc.gz" not in file_name:
		print file_name+" is of unsupported type. Supported type is .warc.gz!"
		return 0
	else:
		return file_name

def getText(html_page):
	soup = BeautifulSoup("<html>"+html_page+"</html>", 'html.parser')
	text = soup.get_text()
	return text	

def runProcedure(html_page, warc_info):
	warc_id = warc_info[0]
	warc_content = warc_info[1]
	warc_index = -1
	
	warc_types = re.findall(warc_type_regex, warc_content)
	warc_records_ids = re.findall(warc_record_id_regex,warc_content)
	
	warc_id = '' 
	text = getText(html_page)
	
    

	##Tokening first into sentences and then into words.
	sentence, tokens = casualTokenizing(text)
			
	##Removing stopwords
	##Couldn't fix an indexing error making it a function, and I'll do it another time.
	# filtered_tokens = filterTokens(tokens)
	'''
	___OPTIONAL PRE-PROCESSING____
	#Stemming
	stemmed_tokens = stemmatizeTokens(tokens)
	
	#Lemmatizing
	lemmatized_tokens=lemmatizeTokens(tokens,"n")

	#Pos-tagging the pre-processed words
	tagged_tokens = pos_tag(tokens)
	'''

	#Chunking
	# chunked_tokens = chunkParser.parse(tagged_tokens)
			
	# runEvaluation()
	#Discovering and tagging Named Entities (NER)
	warc_index+=3
	warc_id = ((warc_records_ids[warc_index][0]).split(' '))[1]
	entities = extractUniqueEntities(tagged_tokens)
	linked_entities = linkEntities(entities)
	return linked_entities

def linkEntities(entities):
	linked_entities=[]
	for entry in entities:
		label = entry.label()
		leaves = entry.leaves()
		size = len(leaves)
		entity = ""
		entity_id = ""
		for leaf in leaves:
			entity += '-'+leaf[0].lower()
		if entity=="":
			continue
		response = os.popen('curl "http://10.149.0.127:9200/freebase/label/_search?q={0}"'.format(entity[1:].strip()))
		try:
			json_res = json.loads(response.read())
		except Exception as e:
			print "No response for "+entity
			continue
		##Here we need to add some validity check in case we get an empty response, to continue to the next entity
		if json_res['hits']['total']==0:
			print "Got 0 hits for: "+entity[1:].strip()
			continue
		max_score = json_res['hits']['max_score']           
		hits = json_res['hits']['hits']
		freebase_id = ""
		for hit in hits:
			print hit
			if hit['_score']==max_score:
				if hit['_index']=="freebase":
					try:
						entity_label = str(hit['_source']['label'])
					except Exception as e:
						continue
					entity_id = '/'+str(hit['_source']['resource'].split('fbase:')[1].replace('.','/'))
		print "================================================"
		if entity_id:
			linked_entities.append({'entity_label':entity_label,'entity_id':entity_id})
		else:
			# print "Got results but no hits for "+entity[1:].strip()
			continue
	return linked_entities			

def main(argv):
	WARC_RECORD_ID = argv[0]
	file_name = validateInput(argv[1])
	output_name = argv[2]
	html_text_array = []

	warc_info = get_warc_info(file_name)

	html_pages_array = re.findall(html_regex, warc_info[1])

	for html_page in html_pages_array:
		html_text_array.append(html_page[0]) 
	
	
	rdd = sc.parallelize(html_text_array, 4)
	linked_entities_rdd = rdd.map(lambda x: runProcedure(x, warc_info))

	linked_entities = linked_entities_rdd.collect()	
	write_file = open(output_name, 'a')

	for linked in linked_entities:

		write_file.write("{0}\t{1}\t{2}\n".format(warc_id,linked['entity_label'],linked['entity_id']))

	write_file.close()			

if __name__ == "__main__":
	argv = sys.argv
	if len(argv)<3:
		print "Provide the .warc.gz file as the first argument along with the script call.\nE.g.: python ~/parser.py sample.warc.gz"
	else:
		main(sys.argv[1:])
	exit(0)
