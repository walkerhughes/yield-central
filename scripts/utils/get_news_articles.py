from datetime import datetime 
import requests 
import heapq 

def get_alphavantage_articles(api_key) -> dict: 
    # get news articles on the macroeconomy that relate to monetary policy 
    url = f'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&topics=economy_monetary&apikey={api_key}'
    response = requests.get(url)
    response_json = response.json()
    return response_json

def get_relevance_score(item: dict) -> float:
    # return relevance score for topic == 'Economy - Monetary'
    if 'topics' in item and isinstance(item['topics'], list):
        for topic in item['topics']:
            if topic['topic'] == 'Economy - Monetary':
                return float(topic['relevance_score'])
    return 0.0

def format_markdown_citation(article: dict) -> str:
    # Extract the date
    date_str = article['time_published']
    date_obj = datetime.strptime(date_str, '%Y%m%dT%H%M%S')
    formatted_date = date_obj.strftime('%Y, %B %d')

    # Extract other details
    authors = ', '.join(article['authors'])
    title = article['title']
    source = article['source']
    url = article['url']
    
    # Format the citation with the title as a hyperlink
    citation = f"{authors}. ({formatted_date}). {title}. {source}. [Read here.]({url})"
    return citation

def get_top_k_relevant_articles(data: dict, k: int) -> list: 
    # return k most relevant articles based on highest relevance scores 
    return heapq.nlargest(k, data["feed"], key = get_relevance_score)

def get_top_k_summaries(top_k_articles: list) -> str: 
    # get summaries for the top k articles retrieved 
    return "\n\n".join([f"Title: {article['title']}\nSummary: {article['summary']}" for article in top_k_articles])

def get_top_k_citations(top_k_articles: list) -> str: 
    # format citations for the top k articles retrieved 
    return "**Sources + Relevant Articles**\n\n" + "\n\n".join([format_markdown_citation(article) for article in top_k_articles])