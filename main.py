# from Bio import Entrez

# # Provide your email address to the NCBI server to identify yourself
# Entrez.email = "bekture18@itu.edu.tr"
#
# # Search for papers containing the term "text mining"
# handle = Entrez.esearch(db="pubmed", term="health", retmax=10)
#
# # Parse the search results and get a list of IDs for the matching papers
# record = Entrez.read(handle)
# id_list = record["IdList"]
#
# # Download the papers in XML format
# for paper_id in id_list:
#     handle = Entrez.efetch(db="pubmed", id=paper_id, rettype="xml")
#     paper_xml = handle.read()
#     # process the paper_xml here as desired
from Bio import Entrez
from lxml import etree
from preprocess import pre_process
# email address required by NCBI
Entrez.email = "bekture18@itu.edu.tr"

# search for papers with keyword "cancer" in the title
handle = Entrez.esearch(db="pubmed", term="cancer[Title]", retmax=444)
record = Entrez.read(handle)
id_list = record["IdList"]
handle = Entrez.efetch(db='pubmed', id=id_list, rettype="abstract", retmode="text")
results = handle.read()
result_list = results.split('\n\n\n')
s_line_results = [abst.replace('\n', ' ').replace('\r', '').strip() for abst in result_list]
# download the abstracts for each article and save them to a file
with open("data/abstracts_100mb.txt", "w", encoding="utf-8") as f:
    # for article_id in record["IdList"]:
    #     # download abstract as plain text
    #     handle = Entrez.efetch(db="pubmed", id=article_id, rettype="abstract", retmode="text")
    #     abstract = handle.read()
    #     pre_processed = pre_process(abstract=abstract)
    #     # write abstract to file
    #     f.write(pre_processed + "\n")
    for abst in s_line_results:
        f.write(abst + "\n")

from pyspark.ml import Pipeline
from sparknlp.annotator import *
from sparknlp.common import *
from sparknlp.base import *
import sparknlp
from sparknlp.pretrained import PretrainedPipeline

# initialize Spark and load pre-trained NER model
spark = sparknlp.start()
ner_dl = PretrainedPipeline('recognize_entities_dl', lang='en')


data = spark.createDataFrame([(abstract,)], ['text'])

# apply NER to the dataframe
result = ner_dl.transform(data)

# extract named entities
entities = [(row.result, row.metadata['entity']) for row in result.select("result", "metadata").collect()[0]['result']]

print(entities)
