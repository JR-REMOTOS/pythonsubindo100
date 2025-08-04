import requests
import json
import os
from hashlib import md5
import re

TMDB_API_KEY = 'ee4baf951a5ef055957a410f05f90eae'
TMDB_BASE_URL = 'https://api.themoviedb.org/3'
TMDB_IMAGE_BASE_URL = 'https://image.tmdb.org/t/p/w500'
CACHE_FILE = 'tmdb_cache.json'

def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_cache(cache):
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=4)

def search_tmdb(title, content_type):
    cache = load_cache()
    cache_key = md5(f"{title}-{content_type}".encode()).hexdigest()
    
    if cache_key in cache:
        return cache[cache_key]

    clean_title = re.sub(r'\s*S\d+E\d+$', '', title, flags=re.I)
    clean_title = re.sub(r'\s*\(\d{4}\)', '', clean_title)
    clean_title = re.sub(r'(dublado|legendado|hd|4k|1080p|720p|\[.*?\])', '', clean_title, flags=re.I)
    clean_title = ' '.join(clean_title.split())

    year = None
    year_match = re.search(r'(\d{4})', title)
    if year_match:
        year = year_match.group(1)
        clean_title = re.sub(r'\s*' + year, '', clean_title).strip()

    endpoint = 'movie' if content_type == 'filme' else 'tv'
    url = f"{TMDB_BASE_URL}/search/{endpoint}?api_key={TMDB_API_KEY}&query={clean_title}&language=pt-BR"
    if year:
        url += f"&year={year}"
    
    response = requests.get(url, verify=False)
    if response.status_code == 200:
        data = response.json()
        if data.get('results'):
            result = data['results'][0]
            poster = f"{TMDB_IMAGE_BASE_URL}{result['poster_path']}" if result.get('poster_path') else None
            genres = result.get('genre_ids', [])
            origin_country = result.get('origin_country', [])
            
            category = content_type
            if content_type == 'serie':
                if 16 in genres:
                    category = 'anime'
                elif 10766 in genres:
                    category = 'novela'
                elif 'KR' in origin_country or 'JP' in origin_country:
                    category = 'dorama'
            
            result_data = {'capa': poster, 'categoria': category}
            cache[cache_key] = result_data
            save_cache(cache)
            return result_data
    
    result_data = {'capa': None, 'categoria': content_type}
    cache[cache_key] = result_data
    save_cache(cache)
    return result_data