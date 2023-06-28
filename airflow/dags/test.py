import os
import openai
import requests
from google.cloud import storage
import pinecone

def fetch_file_from_gcs(bucket_name, file_name):
    # Create a client and get the bucket
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    # Specify the file path in the bucket
    blob = bucket.blob(file_name)

    # Download the file contents
    file_contents = blob.download_as_text()

    # Process the file contents as needed
    # print(f"Contents of '{file_name}':")
    # print(file_contents.encode('utf-8'))
    return file_contents.encode('utf-8')
# Example usage
bucket_name = 'damg-github-dump'
file_name = '20150226_ANSS/text.txt'


def summarize_docs(openaikey, fetched_file):
    openai.api_key = openaikey

    promptstr="Summarize the document:"+fetched_file
# promptstr="Convert my short hand into a first-hand account of the meeting:"+fetch_file_from_gcs(bucket_name, file_name).decode()
    response = openai.Completion.create(
        model="text-davinci-003",
        prompt=promptstr,
        temperature=0,
        max_tokens=64,
        top_p=1.0,
        frequency_penalty=0.0,
        presence_penalty=0.0
        )

fetched_file = fetch_file_from_gcs(bucket_name, file_name).decode()
summarize_docs("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", fetched_file)

def generate_openai_embeddings(openaikey, fetched_file):
    words=[]
    openai.api_key = openaikey
    model_id = 'text-embedding-ada-002'
    words = fetched_file.split()
    # print(words)
    first_500_words = ' '.join(words[:500])
    # print(first_500_words)
    
    embeddings = openai.Embedding.create(
        input=first_500_words,
        engine=model_id)['data'][0]['embedding']
    # print(embeddings)
    return embeddings


embeddings_openai = generate_openai_embeddings("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", fetched_file)


def store_embeddings_pinecone(pinekey, file_name, embeddings_openai):
    pinecone.init(api_key=pinekey)
    index_name = 'assignment2'  # Specify the name of your index
    pinecone.create_index(index_name)

    embeddings = embeddings_openai  # List of embeddings to be indexed
    ids = file_name  # List of corresponding IDs for each embedding

    pinecone.index(index_name=index_name, data=embeddings, ids=ids)
    index_info = pinecone.info(index_name=index_name)
    print(index_info)

    # query_embedding = [...]  # Embedding for the query
    # top_k = 5  # Number of most similar items to retrieve

    # results = pinecone.query(index_name=index_name, data=query_embedding, top_k=top_k)
    # for result in results:
    #     print(result.id, result.score)

    pinecone.delete_index(index_name=index_name)
    pinecone.deinit()

store_embeddings_pinecone('ab112127-8bc7-4575-9dc9-46a265983602', file_name, embeddings_openai)