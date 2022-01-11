# project_208785246_316221811
**text GCP file**
we created the index using the code from assignment 3 and we changed the following:
-we added DL dict field by using doc _len function that we created
-we added id_title dict field that we used later in the search title function
-we used the files inverted index colab and inverted index GCP from previous assignments (with minor changes)

**run frontend**
we return different results to the query in every function:
- search function: we made the search using the tfidf function. we calculated for each doc and word of the query the tf-idf and used it to rate the docs.
in addition, we used the titles of the documents, and the page view of each doc. 
we added "bonus points" to docs that included more than one word that appeared in the query and we sorted the docs and returned the top 100.
- page view
- search body - reurned results using cosine similarity on tf-idf that we calculated on the index fields (we didnt use the function from previous assignments)
- search title
