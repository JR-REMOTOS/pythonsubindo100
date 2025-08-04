import os
import re
import logging
import requests
import json
from hashlib import md5
from database import get_db_connection

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

TMDB_API_KEY = 'ee4baf951a5ef055957a410f05f90eae'
TMDB_BASE_URL = 'https://api.themoviedb.org/3'
TMDB_IMAGE_BASE_URL = 'https://image.tmdb.org/t/p/w500'
CACHE_DIR = '/www/wwwroot/live.revendaiptv.app.br/M3U/cache'
CACHE_FILE = os.path.join(CACHE_DIR, 'tmdb_cache.json')

os.makedirs(CACHE_DIR, exist_ok=True)

def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_cache(cache):
    try:
        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache, f, indent=4)
    except Exception as e:
        logger.error(f"Erro ao salvar cache: {str(e)}")

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

    try:
        response = requests.get(url, verify=False, timeout=10)
        response.raise_for_status()
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
    except:
        result_data = {'capa': None, 'categoria': content_type}
        cache[cache_key] = result_data
        save_cache(cache)
        return result_data

def process_m3u_content(content, db):
    response = {'success': [], 'exists': [], 'error': []}
    lines = content.splitlines()
    current_entry = {}

    for i, line in enumerate(lines):
        line = line.strip()
        if line.startswith('#EXTINF:'):
            match = re.search(r'#EXTINF:.*tvg-id="([^"]*)".*tvg-name="([^"]*)".*tvg-logo="([^"]*)".*group-title="([^"]*)",(.+)', line)
            if match:
                current_entry = {
                    'tvg-id': match.group(1),
                    'tvg-name': match.group(2),
                    'tvg-logo': match.group(3),
                    'group-title': match.group(4),
                    'channel-name': match.group(5)
                }
            else:
                response['error'].append({'message': f'Linha mal formatada: {line}'})
        elif line.startswith('http') and current_entry:
            current_entry['url'] = line
            result = add_content_to_db(
                db,
                current_entry['tvg-id'],
                current_entry['tvg-name'],
                current_entry['tvg-logo'],
                current_entry['group-title'],
                current_entry['channel-name'],
                current_entry['url']
            )
            response['success'].extend(result.get('success', []))
            response['exists'].extend(result.get('exists', []))
            response['error'].extend(result.get('error', []))
            current_entry = {}

    return response

def list_m3u_files(directory):
    return [f for f in os.listdir(directory) if f.endswith('.m3u')]

def reprocess_file(file_path, db):
    response = {'success': [], 'exists': [], 'error': []}
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        entries = process_m3u_content(content, db)
        response['success'].extend(entries.get('success', []))
        response['exists'].extend(entries.get('exists', []))
        response['error'].extend(entries.get('error', []))
        return response
    except Exception as e:
        response['error'].append({'message': str(e)})
        return response

def extract_season_episode(title):
    season = re.search(r'[sS](\d+)', title, re.I)
    episode = re.search(r'[eE](\d+)', title, re.I)
    base_title = re.sub(r'[sS]\d+[eE]\d+', '', title, flags=re.I).strip()
    return {
        'base_title': base_title,
        'season': season.group(1) if season else None,
        'episode': episode.group(1) if episode else None
    }

def generate_directory(title):
    return re.sub(r'[^a-zA-Z0-9]', '-', title.lower())

def get_category_id(db, content_type):
    category_map = {
        'filme': 1,
        'serie': 17,
        'novela': 18,
        'anime': 19,
        'dorama': 20,
        'infantil': 21,
        'canal': 38
    }
    return category_map.get(content_type, 38)

def add_content_to_db(db, tvg_id, tvg_name, tvg_logo, group_title, channel_name, url):
    response = {'success': [], 'exists': [], 'error': []}
    cursor = None

    try:
        cursor = db.cursor(dictionary=True, buffered=True)

        cursor.execute("SELECT player_id, player_midia_id FROM midia_players WHERE player_url = %s", (url,))
        existing_player = cursor.fetchone()

        if existing_player:
            media_id = existing_player['player_midia_id']
            cursor.execute("SELECT midia_titulo FROM midia WHERE midia_id = %s", (media_id,))
            media = cursor.fetchone()
            response['exists'].append({
                'type': 'unknown',
                'data': {'titulo': media['midia_titulo'], 'groupTitle': group_title, 'url': url}
            })
            return response

        content_type = 'canal'
        lower_group = (group_title or '').lower()
        if 'filme' in lower_group:
            content_type = 'filme'
        elif 'serie' in lower_group or 'série' in lower_group:
            content_type = 'serie'
        elif 'infantil' in lower_group:
            content_type = 'infantil'

        raw_title = (channel_name or tvg_name or 'Sem Título').strip()
        title_data = extract_season_episode(raw_title)
        title = title_data['base_title'][:255]
        directory = generate_directory(title)
        synopsis = "Conteúdo importado de uma lista M3U Nenhum Conteudo é Hospedado em Nossos Servidores."

        tmdb_data = search_tmdb(title, content_type)
        image = tmdb_data['capa'] if tmdb_data['capa'] else (tvg_logo or 'default_image.png')
        content_type = tmdb_data['categoria']

        cursor.execute("SELECT midia_id FROM midia WHERE midia_titulo = %s AND midia_tipo = %s", (title, content_type))
        existing_media = cursor.fetchone()

        if existing_media:
            media_id = existing_media['midia_id']
            cursor.execute("SELECT player_id FROM midia_players WHERE player_midia_id = %s AND player_url = %s", (media_id, url))
            if cursor.fetchone():
                response['exists'].append({
                    'type': content_type,
                    'data': {'titulo': raw_title, 'groupTitle': group_title, 'url': url}
                })
                return response
        else:
            cursor.execute("""
                INSERT INTO midia (midia_titulo, midia_image, midia_background, midia_sinopse, 
                                midia_categoria, midia_tipo, midia_diretorio, midia_visualizacoes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 0)
            """, (title, image, image, synopsis, get_category_id(db, content_type), content_type, directory))
            media_id = cursor.lastrowid

        if content_type in ['serie', 'novela', 'anime', 'dorama'] and title_data['season'] and title_data['episode']:
            season_title = f"{title_data['season']}ª Temporada"
            season_dir = generate_directory(f"temporada-{title_data['season']}")

            cursor.execute("SELECT temporada_id FROM midia_temporadas WHERE temporada_midia_id = %s AND temporada_titulo = %s", 
                         (media_id, season_title))
            season = cursor.fetchone()

            if not season:
                cursor.execute("INSERT INTO midia_temporadas (temporada_titulo, temporada_diretorio, temporada_midia_id) VALUES (%s, %s, %s)",
                             (season_title, season_dir, media_id))
                season_id = cursor.lastrowid
            else:
                season_id = season['temporada_id']

            episode_title = f"Episódio {title_data['episode']}"
            episode_dir = generate_directory(f"episodio-{title_data['episode']}")

            cursor.execute("""
                SELECT episodio_id FROM midia_episodios 
                WHERE episodio_midia_id = %s AND episodio_temporada_id = %s AND episodio_numero = %s
            """, (media_id, season_id, title_data['episode']))
            episode = cursor.fetchone()

            if not episode:
                cursor.execute("""
                    INSERT INTO midia_episodios (episodio_titulo, episodio_diretorio, episodio_midia_id, 
                                              episodio_temporada_id, episodio_numero)
                    VALUES (%s, %s, %s, %s, %s)
                """, (episode_title, episode_dir, media_id, season_id, title_data['episode']))
                episode_id = cursor.lastrowid
            else:
                episode_id = episode['episodio_id']

            cursor.execute("""
                INSERT INTO midia_players (player_midia_id, player_temporada_id, player_episodio_id, 
                                        player_titulo, player_url, player_tipo, player_audio, player_acesso)
                VALUES (%s, %s, %s, %s, %s, 'iframe', 'dublado', 'gratis')
            """, (media_id, season_id, episode_id, f"{title} - S{title_data['season']}E{title_data['episode']}", url))
            response['success'].append({'type': content_type, 'data': {'titulo': raw_title, 'groupTitle': group_title, 'url': url}})
        else:
            cursor.execute("""
                INSERT INTO midia_players (player_midia_id, player_titulo, player_url, player_tipo, player_audio, player_acesso)
                VALUES (%s, %s, %s, 'iframe', 'dublado', 'gratis')
            """, (media_id, title, url))
            response['success'].append({'type': content_type, 'data': {'titulo': raw_title, 'groupTitle': group_title, 'url': url}})

    except Exception as e:
        response['error'].append({'type': content_type, 'data': {'titulo': raw_title, 'groupTitle': group_title, 'url': url}, 'message': str(e)})
        raise
    finally:
        if cursor:
            cursor.close()

    return response