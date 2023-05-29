# from Bio import Entrez
# from lxml import etree
# from preprocess import pre_process
# import sys
# import os
#
# Entrez.email = "bekture18@itu.edu.tr"
# file_size = 0
# TARGET_SIZE = 100_000_000
# # initialize the counter and set of unique abstracts
# counter = 0
# unique_abstracts = set()
# downloaded_ids = set()
#
# # open the output file for writing
# with open("abstracts_100mb.txt", "w", encoding="utf-8") as f:
#     # continue searching and downloading abstracts until the target number is reached
#     while file_size < TARGET_SIZE:
#         # search for papers with keyword "cancer" in the title and exclude downloaded IDs
#         handle = Entrez.esearch(db="pubmed", term="cancer[Title]", retmax=9999, retstart=len(downloaded_ids),
#                                 idtype="acc")
#         record = Entrez.read(handle)
#         id_list = record["IdList"]
#
#         # exclude downloaded IDs from subsequent searches
#         id_list = list(set(id_list) - downloaded_ids)
#         downloaded_ids.update(set(id_list))
#
#         # download the abstracts
#         handle = Entrez.efetch(db='pubmed', id=id_list, rettype="abstract", retmode="text")
#         results = handle.read()
#         result_list = results.split('\n\n\n')
#         single_line_results = [abst.replace('\n', ' ').replace('\r', '').strip() for abst in result_list]
#         for abst in single_line_results:
#             f.write(abst + "\n")
#         file_size = os.fstat(f.fileno()).st_size
#         print(file_size)
#         #file_size += sys.getsizeof(single_line_results)
#         if file_size >= TARGET_SIZE:
#             break
#
#         # check if all abstracts have been downloaded
#         if len(id_list) < 9997:
#             break
#
# # print the final number of unique abstracts downloaded
# print(f"Number of unique abstracts: {len(unique_abstracts)}")
import os
import subprocess

# set the email address to identify yourself to NCBI
email = "bekture18@itu.edu.tr"

# open the file for writing
with open("data/abstracts_100mb.txt", "w", encoding="utf-8") as f:
    # set the initial retmax and retstart
    retmax = 40000
    retstart = 0
    downloaded_ids = set()
    TARGET_SIZE = 100_000_000
    # set the initial size of the file
    file_size = 0
    # loop until the file size is greater than or equal to 100MB
    while file_size < TARGET_SIZE:
        os.environ['PATH'] = '/Users/erce/edirect:${PATH}'
        # use ESearch to retrieve the list of IDs matching the query
        cmd_esearch = ["esearch", "-db", "pubmed", "-query", "cancer[Title]", "-retmax", str(retmax)]
        output_esearch = subprocess.run(cmd_esearch, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # parse the list of IDs from the output of ESearch
        id_list = output_esearch.stdout.decode().strip().split("\n")
        id_list = list(set(id_list) - downloaded_ids)
        downloaded_ids.update(set(id_list))
        # use EFetch to retrieve the abstracts
        cmd_efetch = ["efetch", "-db", "pubmed", "-id", ",".join(id_list), "-format", "abstract"]
        output_efetch = subprocess.run(cmd_efetch, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # parse the abstracts from the output of EFetch
        abstract_list = output_efetch.stdout.decode().strip().split("\n\n")
        # write each abstract to the file and update the file size
        for abstract in abstract_list:
            if abstract not in f:
                f.write(abstract + "\n")
                file_size += len(abstract.encode())
        # update the retstart and retmax for the next iteration
        retstart += retmax
