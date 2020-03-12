import numpy as np
import pandas as pd
import requests
from tqdm import tqdm
from tqdm import tqdm, tqdm_notebook
from functools import reduce
from datetime import datetime

# Extract data online

# Get all leading news feed via newsapi.org
def getSources():
    source_url = 'https://newsapi.org/v1/sources?language=en'
    response = requests.get(source_url).json()
    sources = []
    for source in response['sources']:
        sources.append(source['id'])
    return sources

# Get the categories of each source
def mapping():
    d = {}
    response = requests.get('https://newsapi.org/v1/sources?language=en')
    response = response.json()
    for s in response['sources']:
        d[s['id']] = s['category']
    return d


sources = getSources()
m = mapping()


def category(source, m):
    try:
        return m[source]
    except:
        return 'NC'


def getDailyNews():
    sources = getSources()
    key = '1a3246fbcea34573a649ad0bddbe7d64'
    url = 'https://newsapi.org/v1/articles?source={0}&sortBy={1}&apiKey={2}'
    responses = []
    for i, source in tqdm_notebook(enumerate(sources), total=len(sources)):
        
        
        try:
            u = url.format(source, 'top', key)
        except:
            u = url.format(source, 'latest', key)
        
        response = requests.get(u)
               
        r = response.json()
        try:
            for article in r['articles']:
                article['source'] = source
            responses.append(r)
        except:
            print('Rate limit exceeded ... please wait and retry in 6 hours')
            return None
                
    articles = list(map(lambda r: r['articles'], responses))
    articles = list(reduce(lambda x,y: x+y, articles))
    
    news = pd.DataFrame(articles)
    news = news.dropna()
    news = news.drop_duplicates()
    news.reset_index(inplace=True, drop=True)
    d = mapping()
    news['category'] = news['source'].map(lambda s: category(s, d))
    news['scraping_date'] = datetime.now()

    try:
        aux = pd.read_csv(r'C:\Users\alapatt\DSAssignments\Big Data\news.csv')
        aux = aux.append(news)
        aux = aux.drop_duplicates('url')
        aux.reset_index(inplace=True, drop=True)
        aux.to_csv(r'C:\Users\alapatt\DSAssignments\Big Data\news.csv', encoding='utf-8', index=False)
    except:
        news.to_csv(r'C:\Users\alapatt\DSAssignments\Big Data\news.csv', index=False, encoding='utf-8')
        
    print('Done')




def token_lemma(sentence):
    if pd.isna(sentence): # Check if sentence is NaN
        output = sentence
    else:
        output = nltk.word_tokenize(sentence) # Tokenize
        output = [w for w in output if not w in stop_words] 
        output = [w.lower() for w in output] # Lower case
        output = [lemmatizer.lemmatize(w) for w in output] # Lemmatizer
    return output

def _removeNonAscii(s): 
    return "".join(i for i in s if ord(i)<128)

def clean_text(text):
    text = text.lower()
    text = re.sub(r"what's", "what is ", text)
    text = text.replace('(ap)', '')
    text = re.sub(r"\'s", " is ", text)
    text = re.sub(r"\'ve", " have ", text)
    text = re.sub(r"can't", "cannot ", text)
    text = re.sub(r"n't", " not ", text)
    text = re.sub(r"i'm", "i am ", text)

    text = re.sub(r"\'re", " are ", text)
    text = re.sub(r"\'d", " would ", text)
    text = re.sub(r"\'ll", " will ", text)
    text = re.sub(r'\W+', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r"\\", "", text)
    text = re.sub(r"\'", "", text)    
    text = re.sub(r"\"", "", text)
    text = re.sub('[^a-zA-Z ?!]+', '', text)
    text = _removeNonAscii(text)
    text = text.strip()
    return text


# Call main routine

if __name__=='__main__':
    getDailyNews()