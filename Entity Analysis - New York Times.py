
'''
The currrent codebase was created after group project collaboration with Orestis Roussos(Ms.C. Software Engineering)
2016, December
'''

import json
import gzip
import sys
import re
import pprint
import requests
import nltk
import os
from nltk import sent_tokenize, word_tokenize, pos_tag, ne_chunk, tree
from nltk.corpus import stopwords
from nltk.tag.stanford import StanfordNERTagger
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nytimesarticle import articleAPI
from pprint import pprint
from bs4 import BeautifulSoup
import pandas as pd
import matplotlib.pyplot as plt;plt.rcdefaults()
import matplotlib as mpl
import numpy as np
import matplotlib.lines as mlines
import sys
import operator

reload(sys)  
sys.setdefaultencoding('utf8')


api_key = "01e07bd2b8034b81bc9bef8d3e35df3a"
api_interface = articleAPI(api_key)


warc_type_regex = re.compile(r'((WARC-Type:).*)')
warc_record_id_regex = re.compile(r'((WARC-Record-ID:) <.*>)')
html_regex = re.compile(r'<html\s*(((?!<html|<\/html>).)+)\s*<\/html>', re.DOTALL)

def visualize(main_entity_name, article_name, article_entities, output_name, bar_amount, sentiment=None):
		#Initialize Values
		plt.figure(num=None,figsize=(12,7),dpi=80)
		encoding_regex = re.compile(r"0x([a-zA-Z0-9]+)")
		article_name = encoding_regex.sub("",article_name)
		dictionary = dict()
		entities = list()
		values = list()
		for element in article_entities:
			if element == []:
				break
			else:
				dictionary[element['entity_label']] = element['count']
				# dictionary[element['initial_label']] = element['count']
		#bar_amount check
		if(bar_amount != 0):
			#Sort Dictionary As Iteretable .items(list) ,reverse=True Descending
			temp_sort_list = list(sorted(dictionary.items(),key=operator.itemgetter(1), reverse=True))
			
			counter=0
			#Create Sorted Entities - Values Lists
			for list_item in temp_sort_list:
				if(counter == len(dictionary)-1 or counter == bar_amount):
					break
				entities.append(list_item[0])
				values.append(list_item[1])
				counter+=1
		
		#Initialize the pd. set with Values - Keys
		s = pd.Series(
			values,
			entities
		)
		mpl.rcParams['legend.numpoints'] = 1
		#remove the second legend marker
		#Set descriptions:
		plt.title(article_name)
		plt.ylabel('Extracted Entities')
		plt.xlabel('Popularity', color='blue')

		#Set tick colors:
		ax = plt.gca()
		ax.tick_params(axis='x', colors='blue')
		ax.tick_params(axis='y', colors='purple')

		
		#Plot the data:
		my_colors = 'rgbkymc'  #red, green, blue, black, etc. palete
		
		#plot Horizontal Bar & Colors & Legend
		s.plot( 
			kind='barh', 
			color=my_colors,
			legend=True,
		)
		#MAX RANKED ENTITY
		max_val = values.index(max(values))
		
		#Find the corresponded key/entity_name in the dictionary for the max valued entity 
		counter=0
		Entity = ''
		for j in entities:
			if counter == max_val:
				Entity = j
				break
			else:
				counter+=1

		#Adjust the entity in color palette (rgbkymc = 7)
		while( max_val - 7 >= 0): max_val-=7
		
		#Add Line2D objects to the Legend [Empty Line x2 , STAR]
		encoding_regex = re.compile(r"0x([a-zA-Z0-9]+)")
		article_name = encoding_regex.sub("",article_name)

		# Article_name = mlines.Line2D([], [], color="b", marker=' ',
							  # markersize=15, label="Article Name: " + article_name)
		Entity_name = mlines.Line2D([], [], color="white", marker=' ',
							  markersize=15, label="Related To: " + main_entity_name)
		Popular_Entity = mlines.Line2D([], [], color="rgbkymc"[max_val], marker='*',
							  markersize=15, label="Popular Entity: " + Entity)
		
		if sentiment:
			neg = mlines.Line2D([], [], color="red", marker=' ', markersize=15, label="Negative: " + str(sentiment['neg']))
			neu = mlines.Line2D([], [], color="blue", marker=' ', markersize=15, label="Neutral: " + str(sentiment['neu']))
			pos = mlines.Line2D([], [], color="green", marker=' ', markersize=15, label="Positive: " + str(sentiment['pos']))
			plt.legend(handles=[Entity_name,Popular_Entity,neg,neu,pos])
		else:
			#Initialize Legend's items and add it to Plot
			plt.legend(handles=[Article_name,Entity_name,Popular_Entity],loc='upper center', bbox_to_anchor=(0.5,-0.1))

		#Export 
		plt.savefig("./output_plots/"+output_name+".png")
		

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

def casualTokenizing(text):
	mr_regex = re.compile(r"(Mr\.|Mrs\.|Ms\.)",flags=re.I)
	text = mr_regex.sub("",text)
	sentences = sent_tokenize(text)
	sentences = filter(lambda sent: sent != "", sentences)
	tokens = word_tokenize(text)

	return sentences, tokens

def filterTokens(tokens):
	stop_words = set(stopwords.words("english"))
	filtered_tokens = []
	for t in tokens:
		if t not in stop_words:
			filtered_tokens.append(t)
	return filtered_tokens

def extractUniqueEntities(tokens):
	unique_entities = []
	entity_count = {}
	tagged_entities = ne_chunk(tokens, binary=False)
	# st = StanfordNERTagger('./stan_files/english.all.3class.distsim.crf.ser.gz','./stan_files/stanford-ner.jar')
	# print st.tag("The Washington Monument is the most prominent structure in Washington, D.C. and one of the city's early attractions. It was built in honor of George Washington, who led the country to independence and then became its first President.".split())
	for entity in tagged_entities:
		if isinstance(entity, tree.Tree):
			if (entity.label()!="PERSON"):
				continue
			if entity not in unique_entities:
				unique_entities.append(entity)
			entity_buff=""
			for leaf in entity.leaves():
				entity_buff+=leaf[0]+" "
			entity_buff = entity_buff.strip()
			# In case of case sensitivity issues, replace this with the bellow code.
			# if entity_buff.lower() not in entity_count.keys():
			# 	entity_count[entity_buff.lower()] = 1
			# else:
			# 	entity_count[entity_buff.lower()] += 1
			if entity_buff not in entity_count.keys():
				entity_count[entity_buff] = 1
			else:
				entity_count[entity_buff] += 1
	return unique_entities, entity_count


def linkEntities(entities, entity_count):
	linked_entities=[]
	for entry in entities:
		label = entry.label()
		leaves = entry.leaves()
		size = len(leaves)
		entity = ""
		raw_entity = ""
		entity_id = ""
		raw_matched=0
		for leaf in leaves:
			entity += '-'+leaf[0].lower()
			raw_entity += " "+leaf[0].lower()
		if entity=="":
			continue

		for l in linked_entities:
			if raw_entity in l['entity_label'].lower():
				l['count']+=1
				raw_matched=1
				# print raw_entity+" -> "+l['entity_label']
				# raw_input()
		if raw_matched==1:
			continue

		response = os.popen('curl "http://10.149.0.127:9200/freebase/label/_search?q={0}"'.format(entity[1:].encode('utf-8').strip()))
		try:
			json_res = json.loads(response.read())
		except Exception as e:
			print "No response for "+entity
			continue
		#Here we need to add some validity check in case we get an empty response, to continue to the next entity
		if json_res['hits']['total']==0:
			print "Got 0 hits for: "+entity[1:].strip()
			continue
		max_score = json_res['hits']['max_score']
		hits = json_res['hits']['hits']
		freebase_id = ""
		for hit in hits:
			# print hit
			# if hit['_score']==max_score:
			if hit['_score']==max_score and max_score>=6:
				# print hit
				if hit['_index']=="freebase":
					try:
						entity_label = str(hit['_source']['label'])
						entity_label = entity_label.split(", ")[0]
					except Exception as e:
						continue
					entity_id = '/'+str(hit['_source']['resource'].split('fbase:')[1].replace('.','/'))
		# print "================================================"
		if entity_id:
			entity_buff = ""
			for leaf in entry.leaves():
				entity_buff+=leaf[0]+" "
			entity_buff = entity_buff.strip()
			linked_entities.append({'entity_label':entity_label,'entity_id':entity_id, 'initial_label':entity_buff, 'count':entity_count[entity_buff]})
		else:
			# print "Got results but no hits for "+entity[1:].strip()
			continue
	return linked_entities

def get_articles(args):
	entity = args
	articles = api_interface.search( 
		q = entity,
		fq = {'headline': entity,'body': entity,'source':'The New York Times'},
		sort = 'newest',
		begin_date=20081125
		# Date 2008/11/25 onwards
		#filter query : Headline Related to Obama
	)
	return articles
'''
## ============Article Keys=============
##[u'type_of_material', u'blog', u'news_desk', u'lead_paragraph', u'headline', u'abstract',\
## u'print_page', u'word_count', u'_id', u'snippet', u'source', u'slideshow_credits', u'web_url',\
## u'multimedia', u'subsection_name', u'keywords', u'byline', u'document_type', u'pub_date', u'section_name']
'''
def filter_articles(articles):
	docs = articles['response']['docs']
	filtered = []
	for d in docs:
		d['headline']['main'] = d['headline']['main'].encode('utf-8')
		keyword_buffer = []
		for k in d['keywords']:
			if (k['name']!='subject'):
				keyword_buffer.append(k)

		d['keywords'] = keyword_buffer
		d.pop('multimedia', None)
		d.pop('slideshow_credits', None)
		d.pop('snippet', None)
		d.pop('lead_paragraph', None)
		if (d['section_name'].encode('utf-8') != 'Opinion'):
			filtered.append(d)
		# filtered.append(d)
		
	return filtered

def runProcedure(argv):
	WARC_RECORD_ID = argv[0]
	file_name = validateInput(argv[1])
	output_name = argv[2]

	with gzip.open(file_name, 'rb') as f:
		warc_id = "Warc_id pending."
		warc_content = f.read()

		warc_types = re.findall(warc_type_regex, warc_content)
		warc_records_ids = re.findall(warc_record_id_regex,warc_content)
		warc_index = -1
		#Getting all html text and putting it in the responsive array.
		html_pages_array = re.findall(html_regex, warc_content)

		#For each element in array:
		write_file = open(output_name, 'w')
		# write_file.write("{0}\t{1}\t{2}\n".format("WARC-RECORD-ID","Entity Labe;","Freebase Entity ID"))
		write_file.close()
		for html_page in html_pages_array:
			warc_id=''
			#Extracting all text with BS
			#I'm appending the tags to the front and the back as they are getting stripped cause of our warc regex.
			text = getText(html_page[0])

			#Tokening first into sentences and then into words.
			sentence, tokens = casualTokenizing(text)
			tokens = filterTokens(tokens)
			tagged_tokens = pos_tag(tokens)

			#Discovering and tagging Named Entities (NER)
			warc_index+=3
			# warc_id = ((warc_records_ids[warc_index][0]).split(' '))[1]
			
			entities, entity_count = extractUniqueEntities(tagged_tokens)
			linked_entities = linkEntities(entities, entity_count)
			article_sentiment = []
			total_sent_list = []
			sid = SentimentIntensityAnalyzer()
			
			for entity in linked_entities:	#linked_entities should go here.
				query_entity = entity['entity_label']

				# query_entity = entity['initial_label']
				api_response = get_articles(query_entity)
				# articles = toy_parser(api_response)
				filtered_articles = filter_articles(api_response)
				overall_entity_score = []
				r = ''
				print len(api_response['response']['docs'])
				print len(filtered_articles)
				file_count = 0
				for article in filtered_articles:
					article_entities = []
					article_entities_count = {}
					article_linked_entities = []
					total_art_sent = {}
					print article['web_url']
					article_name = article['headline']['main']
					r = requests.get(article['web_url'])
					soup = BeautifulSoup(r.text, 'html.parser')
					# text = soup.get_text('p class="story-body-text story-content"')
					text = soup.find_all('p', {'class':'story-body-text story-content'})

					merged_text = ''
					for t in text:
						# print t.get_text()
						merged_text+=t.get_text()
					
					# merged_text=merged_text.decode('utf-8').encode('utf-8')
					article_sentences, article_tokens = casualTokenizing(merged_text)

					# sent_array = []
					# total_sent = {'neg' : 0, 'pos' : 0, 'neu' : 0, 'comp' : 0}
					# for article_sent in article_sentences:
					# 	sa = sid.polarity_scores(article_sent)
					# 	sent_array.append({article_sent:sa})
					# 	total_sent['neg'] +=sa['neg']
					# 	total_sent['pos'] +=sa['pos']
					# 	total_sent['neu'] +=sa['neu']
					# 	total_sent['comp'] += sa['compound']

					# total_art_sent[article_name]=total_sent
					# total_sent_list.append(total_art_sent)

					sa = sid.polarity_scores(merged_text)
					article_sentiment.append({article_name:sa})


					article_tagged_tokens = pos_tag(article_tokens)
					article_entities, article_entities_count = extractUniqueEntities(article_tagged_tokens)
					article_linked_entities = linkEntities(article_entities, article_entities_count)
					if not overall_entity_score:
						overall_entity_score = article_linked_entities
					else:
						for l in article_linked_entities:
							flag = 0
							for o in overall_entity_score:
								if l['entity_label']==o['entity_label']:
									o['count'] += l['count']
									flag = 1
							if flag == 0:
								overall_entity_score.append(l)
					print "\n------ Title: "+article['headline']['main']+" ----------"
					# print "***Article entities found, along with occurance rate:***"
					# pprint(article_linked_entities)
					# print "\n(press return for next article.)"
					# raw_input()
					if not article_linked_entities:
						print article['headline']['main']
						print article_linked_entities
						continue
					if not query_entity:
						continue
					try:
						visualize(query_entity, article['headline']['main'].encode(utf-8), article_linked_entities, query_entity.split(" ")[0]+"_"+str(file_count), 5, sa)
						file_count+=1
					except Exception as e:
						visualize(query_entity, article['headline']['main'], article_linked_entities, query_entity.split(" ")[0]+"_"+str(file_count), 5, sa)
						file_count+=1
				if not overall_entity_score:
					continue
				try:
					visualize(query_entity, "Overall Correlation", overall_entity_score, query_entity, 10)
				except ValueError:
					print "Overall Correlation /Error"
	return 0



def main(argv):
	runProcedure(argv)
	exit(0)



if __name__ == "__main__":
	argv = sys.argv
	if len(argv)<3:
		print "Provide the .warc.gz file as the second argument along with the script call.\nE.g.: python ~/parser.py sample.warc.gz"
	else:
		main(sys.argv[1:])
	exit(0)