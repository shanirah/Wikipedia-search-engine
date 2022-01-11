from flask import Flask, request, jsonify
from pathlib import Path
import pickle
from inverted_index_gcp import *
import os
from google.cloud import storage
remove =["#","@","?","!","$","%","^","*","&","(",")","-","=","+","/",".",",",":","\",","|","]","[","{","}","<",">",";","_", "'", "\"" ]


#body inverted index
os.environ["GCLOUD_PROJECT"] = "ass3-334513"
bucket_name = "body_index_shani_noa2"
client = storage.Client()
bucket = client.bucket(bucket_name)
index_name = "index.pkl"
blob_index = bucket.blob(f"postings_gcp/{index_name}")
pickle_in = blob_index.download_as_string()
inverted = pickle.loads(pickle_in)

#pageviews
f_name = "pageviews-202108-user.pkl"
#f = open(f_name, 'rb')
#pageview = pickle.load(f)
#pageview_list = [(key,val) for key,val in pageview.items()]
#pageview_sorted = sorted(pageview_list, key = lambda x: x[1],reverse=True)[:100]
#max_pageview = 8000000

class MyFlaskApp(Flask):
    def run(self, host=None, port=None, debug=None, **options):
        super(MyFlaskApp, self).run(host=host, port=port, debug=debug, **options)
        
app = MyFlaskApp(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False


@app.route("/search")
def search():
    ''' Returns up to a 100 of your best search results for the query. This is 
        the place to put forward your best search engine, and you are free to
        implement the retrieval whoever you'd like within the bound of the 
        project requirements (efficiency, quality, etc.). That means it is up to
        you to decide on whether to use stemming, remove stopwords, use 
        PageRank, query expansion, etc.

        To issue a query navigate to a URL like:
        http://YOUR_SERVER_DOMAIN/search?query=hello+world
        where YOUR_SERVER_DOMAIN is something like XXXX-XX-XX-XX-XX.ngrok.io
        if you're using ngrok on Colab or your external IP on GCP.
    Returns:
    --------
        list of up to 100 search results, ordered from best to worst where each 
        element is a tuple (wiki_id, title).
    '''
    res = []
    query = request.args.get('query', '')
    if len(query) == 0:
      return jsonify(res)
    # BEGIN SOLUTION
    query = [query]
    query = query[0].split()
    #query = [RE_WORD.finditer(q) for q in query]
    mydict = {} 
    for q in query:
      for r in remove:
        while q.count(r)>0:
          q=q.replace(r,'')
      locs = inverted.posting_locs[q]
      if len(locs) == 0:
        continue
      with closing(MultiFileReader()) as reader:
          b = reader.read(locs, inverted.df[q] * 6) 
          posting_list = []
          for i in range(inverted.df[q]):
              doc_id = int.from_bytes(b[i*6:i*6+4], 'big')
              tf = int.from_bytes(b[i*6+4:(i+1)*6], 'big')
              posting_list.append((doc_id, tf)) 
      for docid, appearances in posting_list:
          if docid not in mydict.keys():
              mydict[docid] = appearances + (inverted.id_title[docid]).lower().count(q.lower())*150-(len(inverted.id_title[docid])*2)
              #mydict[docid] = appearances +(pageview[docid]/max_pageview) + (inverted.id_title[docid]).lower().count(q.lower())*150-(len(inverted.id_title[docid])*2)
          else:
              mydict[docid] = mydict[docid]+appearances+ inverted.id_title[docid].lower().count(q)*150-(len(inverted.id_title[docid])*2)
    appearances_per_doc = [(key,val) for key,val in mydict.items()]
    sorted_appearances = sorted(appearances_per_doc, key = lambda x: x[1],reverse=True)
    if len(sorted_appearances)>100:
      sorted_appearances = sorted_appearances[:100]
    for doc in sorted_appearances:
      title = inverted.id_title[doc[0]]
      res.append((doc[0],title))
    # END SOLUTION
    return jsonify(res)

@app.route("/search_body")
def search_body():
    ''' Returns up to a 100 search results for the query using TFIDF AND COSINE
        SIMILARITY OF THE BODY OF ARTICLES ONLY. DO NOT use stemming. DO USE the 
        staff-provided tokenizer from Assignment 3 (GCP part) to do the 
        tokenization and remove stopwords. 

        To issue a query navigate to a URL like:
         http://YOUR_SERVER_DOMAIN/search_body?query=hello+world
        where YOUR_SERVER_DOMAIN is something like XXXX-XX-XX-XX-XX.ngrok.io
        if you're using ngrok on Colab or your external IP on GCP.
    Returns:
    --------
        list of up to 100 search results, ordered from best to worst where each 
        element is a tuple (wiki_id, title).
    '''
    res = []
    query = request.args.get('query', '')
    if len(query) == 0:
      return jsonify(res)
    # BEGIN SOLUTION

    # END SOLUTION
    return jsonify(res)

@app.route("/search_title")
def search_title():
    ''' Returns ALL (not just top 100) search results that contain A QUERY WORD 
        IN THE TITLE of articles, ordered in descending order of the NUMBER OF 
        QUERY WORDS that appear in the title. For example, a document with a 
        title that matches two of the query words will be ranked before a 
        document with a title that matches only one query term. 

        Test this by navigating to the a URL like:
         http://YOUR_SERVER_DOMAIN/search_title?query=hello+world
        where YOUR_SERVER_DOMAIN is something like XXXX-XX-XX-XX-XX.ngrok.io
        if you're using ngrok on Colab or your external IP on GCP.
    Returns:
    --------
        list of ALL (not just top 100) search results, ordered from best to 
        worst where each element is a tuple (wiki_id, title).
    '''
    
    res = []
    query = request.args.get('query', '')
    if len(query) == 0:
      return jsonify(res)
    # BEGIN SOLUTION
    query = [query]
    query = query[0].split()
    mydict = {} 
    for q in query:
      for r in remove:
        while q.count(r)>0:
          q=q.replace(r,'')
      locs = inverted.posting_locs[q]
      if len(locs) == 0:
        continue
      with closing(MultiFileReader()) as reader:
          b = reader.read(locs, inverted.df[q] * 6) 
          posting_list = []
          for i in range(inverted.df[q]):
              doc_id = int.from_bytes(b[i*6:i*6+4], 'big')
              tf = int.from_bytes(b[i*6+4:(i+1)*6], 'big')
              posting_list.append((doc_id, tf)) 
      for docid, appearances in posting_list:
        if (inverted.id_title[docid]).lower().count(q.lower())>0:
          if docid not in mydict.keys():
              mydict[docid] = 1
          else:
              mydict[docid] = mydict[docid]+1
    appearances_per_doc = [(key,val) for key,val in mydict.items()]
    sorted_appearances = sorted(appearances_per_doc, key = lambda x: x[1],reverse=True)
    for doc in sorted_appearances:
      title = inverted.id_title[doc[0]]
      res.append((doc[0],title))
    # END SOLUTION
    return jsonify(res)

@app.route("/search_anchor")
def search_anchor():
    ''' Returns ALL (not just top 100) search results that contain A QUERY WORD 
        IN THE ANCHOR TEXT of articles, ordered in descending order of the 
        NUMBER OF QUERY WORDS that appear in anchor text linking to the page. 
        For example, a document with a anchor text that matches two of the 
        query words will be ranked before a document with anchor text that 
        matches only one query term. 

        Test this by navigating to the a URL like:
         http://YOUR_SERVER_DOMAIN/search_anchor?query=hello+world
        where YOUR_SERVER_DOMAIN is something like XXXX-XX-XX-XX-XX.ngrok.io
        if you're using ngrok on Colab or your external IP on GCP.
    Returns:
    --------
        list of ALL (not just top 100) search results, ordered from best to 
        worst where each element is a tuple (wiki_id, title).
    '''
    res = []
    query = request.args.get('query', '')
    if len(query) == 0:
      return jsonify(res)
    # BEGIN SOLUTION
    
    # END SOLUTION
    return jsonify(res)

@app.route("/get_pagerank", methods=['POST'])
def get_pagerank():
    ''' Returns PageRank values for a list of provided wiki article IDs. 

        Test this by issuing a POST request to a URL like:
          http://YOUR_SERVER_DOMAIN/get_pagerank
        with a json payload of the list of article ids. In python do:
          import requests
          requests.post('http://YOUR_SERVER_DOMAIN/get_pagerank', json=[1,5,8])
        As before YOUR_SERVER_DOMAIN is something like XXXX-XX-XX-XX-XX.ngrok.io
        if you're using ngrok on Colab or your external IP on GCP.
    Returns:
    --------
        list of floats:
          list of PageRank scores that correrspond to the provided article IDs.
    '''
    res = []
    wiki_ids = request.get_json()
    if len(wiki_ids) == 0:
      return jsonify(res)
    # BEGIN SOLUTION

    # END SOLUTION
    return jsonify(res)

@app.route("/get_pageview", methods=['POST'])
def get_pageview():
    ''' Returns the number of page views that each of the provide wiki articles
        had in August 2021.

        Test this by issuing a POST request to a URL like:
          http://YOUR_SERVER_DOMAIN/get_pageview
        with a json payload of the list of article ids. In python do:
          import requests
          requests.post('http://YOUR_SERVER_DOMAIN/get_pageview', json=[1,5,8])
        As before YOUR_SERVER_DOMAIN is something like XXXX-XX-XX-XX-XX.ngrok.io
        if you're using ngrok on Colab or your external IP on GCP.
    Returns:
    --------
        list of ints:
          list of page view numbers from August 2021 that correrspond to the 
          provided list article IDs.
    '''
    res = []
    wiki_ids = request.get_json()
    if len(wiki_ids) == 0:
      return jsonify(res)
    # BEGIN SOLUTION
    #for id in wiki_ids:
    #  res.append(pageview[id])
    # END SOLUTION
    return jsonify(res)

        
def read_posting_list(inverted, w):
  locs = inverted.posting_locs[w]
  with closing(MultiFileReader()) as reader:
    b = reader.read(locs, inverted.df[w] * 6) 
    posting_list = []
    for i in range(inverted.df[w]):
      doc_id = int.from_bytes(b[i*6:i*6+4], 'big')
      tf = int.from_bytes(b[i*6+4:(i+1)*6], 'big')
      posting_list.append((doc_id, tf))
  return posting_list

if __name__ == '__main__':
    # run the Flask RESTful API, make the server publicly available (host='0.0.0.0') on port 8080
    app.run(host='0.0.0.0', port=8080, debug=False)
