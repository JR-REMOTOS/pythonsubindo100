import os
import json
import re
import time
import uuid
from threading import Lock
from flask import Flask, Response, render_template, request, jsonify, send_from_directory
from werkzeug.utils import secure_filename
import logging
import mysql.connector
from database import get_db_connection, init_db
from m3u_processor import parse_m3u_stream, list_m3u_files, add_content_to_db
from duplicate_finder import find_duplicates, remove_duplicates
from xtream_api import XtreamAPI

# Configuração de logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
app = Flask(__name__)

# Definir limite de upload (16 MB)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

# Diretório para arquivos M3U
M3U_DIR = 'data/m3u'
os.makedirs(M3U_DIR, exist_ok=True)

# Inicializar banco de dados
try:
    db = get_db_connection()
    init_db(db)
    db.close()
except Exception as e:
    logging.error(f"Erro ao inicializar banco de dados: {str(e)}")
    raise

# Lock para evitar múltiplas requisições simultâneas
upload_lock = Lock()


@app.route('/')
def index():
    return render_template('index.html')

@app.route('/static/<path:filename>')
def serve_static(filename):
    return send_from_directory('static', filename)

@app.route('/api/upload', methods=['POST'])
def upload_file():
    request_id = str(uuid.uuid4())
    try:
        app.logger.info(f"[{request_id}] Iniciando upload de múltiplos arquivos")
        with upload_lock:
            if not os.access(M3U_DIR, os.W_OK):
                app.logger.error(f"[{request_id}] Sem permissão de escrita em {M3U_DIR}")
                return jsonify({'results': {'error': [{'message': 'Sem permissão para salvar arquivos'}]}}), 500

            stat = os.statvfs(M3U_DIR)
            free_space_mb = (stat.f_bavail * stat.f_frsize) / (1024 * 1024)
            if free_space_mb < 10:
                app.logger.error(f"[{request_id}] Espaço insuficiente: {free_space_mb:.2f} MB")
                return jsonify({'results': {'error': [{'message': 'Espaço em disco insuficiente'}]}}), 500

            if 'm3uFile' not in request.files:
                app.logger.error(f"[{request_id}] Nenhum arquivo enviado")
                return jsonify({'results': {'error': [{'message': 'Nenhum arquivo enviado'}]}}), 400

            files = request.files.getlist('m3uFile')
            if not files or all(file.filename == '' for file in files):
                app.logger.error(f"[{request_id}] Nenhum arquivo selecionado")
                return jsonify({'results': {'error': [{'message': 'Nenhum arquivo selecionado'}]}}), 400

            uploaded_files = []
            for file in files:
                original_filename = secure_filename(file.filename)
                if not original_filename.endswith('.m3u'):
                    original_filename += '.m3u'
                base, ext = os.path.splitext(original_filename)
                filename = original_filename
                file_path = os.path.join(M3U_DIR, filename)

                counter = 1
                timestamp = int(time.time())
                while os.path.exists(file_path):
                    filename = f"{base}_{timestamp}_{counter}{ext}"
                    file_path = os.path.join(M3U_DIR, filename)
                    counter += 1

                file.save(file_path)
                app.logger.info(f"[{request_id}] Arquivo {filename} salvo em {file_path}")
                uploaded_files.append(filename)

            return jsonify({'results': {'tempFiles': uploaded_files, 'success': True}})
    except Exception as e:
        app.logger.error(f"[{request_id}] Erro ao fazer upload: {str(e)}", exc_info=True)
        return jsonify({'results': {'error': [{'message': f"Erro ao fazer upload: {str(e)}"}]}}), 500


@app.route('/api/parse_m3u', methods=['POST'])
def parse_m3u():
    request_id = str(uuid.uuid4())
    try:
        app.logger.info(f"[{request_id}] Iniciando streaming de M3U")
        data = request.get_json()
        base_url = data.get('base_url')
        username = data.get('username')
        password = data.get('password')

        if not all([base_url, username, password]):
            return jsonify({'error': 'URL, usuário e senha são obrigatórios'}), 400

        xtream_api = XtreamAPI(base_url, username, password)
        m3u_iterator = xtream_api.download_m3u()

        if not m3u_iterator:
            return jsonify({'error': 'Falha ao baixar a lista M3U'}), 500

        def generate():
            for chunk in m3u_iterator:
                yield chunk

        return Response(generate(), mimetype='text/plain')

    except Exception as e:
        app.logger.error(f"[{request_id}] Erro ao parsear M3U: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/add_items_to_db', methods=['POST'])
def add_items_to_db():
    request_id = str(uuid.uuid4())
    db = None
    try:
        app.logger.info(f"[{request_id}] Adicionando itens ao banco de dados")
        data = request.get_json()
        items = data.get('items')

        if not items:
            return jsonify({'error': 'Nenhum item para adicionar'}), 400

        db = get_db_connection()
        response = {'success': [], 'exists': [], 'error': []}
        
        for item in items:
            result = add_content_to_db(
                db,
                item.get('tvg_id'),
                item.get('tvg_name'),
                item.get('tvg_logo'),
                item.get('group_title'),
                item.get('channel_name'),
                item.get('url')
            )
            response['success'].extend(result.get('success', []))
            response['exists'].extend(result.get('exists', []))
            response['error'].extend(result.get('error', []))
        
        db.commit()
        return jsonify(response)

    except Exception as e:
        app.logger.error(f"[{request_id}] Erro ao adicionar itens ao DB: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    finally:
        if db and db.is_connected():
            db.close()


@app.route('/api/list_files', methods=['GET'])
def list_files():
    request_id = str(uuid.uuid4())
    try:
        app.logger.info(f"[{request_id}] Listando arquivos")
        files = list_m3u_files(M3U_DIR)
        file_results = []

        for file in files:
            progress_file = os.path.join(M3U_DIR, f"{file}.progress.json")
            m3u_path = os.path.join(M3U_DIR, file)

            total = 0
            with open(m3u_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                total = len([i for i, line in enumerate(lines) if line.strip().startswith('#EXTINF:') and i + 1 < len(lines) and lines[i + 1].strip().startswith('http')])

            if os.path.exists(progress_file):
                with open(progress_file, 'r') as f:
                    progress_data = json.load(f)
                processed = progress_data['processed_urls']
                status = 'Incompleto' if processed < total else 'Completo'
            else:
                processed = total
                status = 'Completo'

            file_results.append({
                'name': file,
                'total': total,
                'processed': processed,
                'status': status
            })

        return jsonify({'results': file_results})
    except Exception as e:
        app.logger.error(f"[{request_id}] Erro ao listar: {str(e)}", exc_info=True)
        return jsonify({'results': {'error': [{'message': f"Erro ao listar: {str(e)}"}]}}), 500

@app.route('/api/delete', methods=['POST'])
def delete():
    db = None
    cursor = None
    request_id = str(uuid.uuid4())
    try:
        app.logger.info(f"[{request_id}] Iniciando exclusão")
        filename = request.form.get('deleteFile')
        if not filename:
            app.logger.error(f"[{request_id}] Nenhum arquivo especificado")
            return jsonify({'results': {'error': [{'message': 'Nenhum arquivo especificado'}]}}), 400

        m3u_path = os.path.join(M3U_DIR, filename)
        progress_file = os.path.join(M3U_DIR, f"{filename}.progress.json")

        if not os.path.exists(m3u_path):
            app.logger.error(f"[{request_id}] Arquivo não encontrado: {m3u_path}")
            return jsonify({'results': {'error': [{'message': 'Arquivo não encontrado'}]}}), 404

        db = get_db_connection()
        cursor = db.cursor(dictionary=True)

        titles = []
        total_items = 0
        with open(m3u_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip().startswith('#EXTINF:'):
                    total_items += 1
                    match = re.match(r'#EXTINF:-?\d+(?:.*?(tvg-id="([^"]*)"))?(?:.*?(tvg-name="([^"]*)"))?.*,(.+)', line.strip())
                    if match:
                        channel_name = match.group(4).strip()
                        tvg_name = match.group(3) or ''
                        title = (channel_name or tvg_name or 'Sem Título').strip()[:255]
                        # Normalizar título: remover espaços extras e converter para minúsculas
                        title = ' '.join(title.split()).lower()
                        titles.append(title)
                        app.logger.debug(f"[{request_id}] Título extraído: {title}")

        deleted_items = 0
        db.start_transaction()
        try:
            for title in titles:
                # Normalizar título na query
                cursor.execute("SELECT midia_id FROM midia WHERE LOWER(midia_titulo) = %s", (title,))
                media_rows = cursor.fetchall()
                app.logger.debug(f"[{request_id}] Títulos encontrados para '{title}': {len(media_rows)}")
                for media in media_rows:
                    midia_id = media['midia_id']
                    cursor.execute("DELETE FROM midia_players WHERE player_midia_id = %s", (midia_id,))
                    cursor.execute("DELETE FROM midia_episodios WHERE episodio_midia_id = %s", (midia_id,))
                    cursor.execute("DELETE FROM midia_temporadas WHERE temporada_midia_id = %s", (midia_id,))
                    cursor.execute("DELETE FROM midia WHERE midia_id = %s", (midia_id,))
                    deleted_items += 1
            db.commit()
        except Exception as e:
            db.rollback()
            app.logger.error(f"[{request_id}] Erro durante exclusão no banco: {str(e)}")
            raise

        if os.path.exists(progress_file):
            os.remove(progress_file)
        os.remove(m3u_path)

        app.logger.info(f"[{request_id}] Arquivo {filename} excluído: {deleted_items}/{total_items}")
        return jsonify({
            'results': {
                'success': True,
                'message': f"Arquivo {filename} excluído",
                'processed': deleted_items,
                'total': total_items
            }
        })
    except Exception as e:
        app.logger.error(f"[{request_id}] Erro ao excluir: {str(e)}", exc_info=True)
        return jsonify({'results': {'error': [{'message': f"Erro ao excluir: {str(e)}"}]}}), 500
    finally:
        if cursor:
            cursor.close()
        if db and db.is_connected():
            db.close()
            
            

@app.route('/api/get_all_content', methods=['GET'])
def get_all_content():
    db = None
    try:
        db = get_db_connection()
        cursor = db.cursor(dictionary=True)
        
        query = "SELECT midia_id, midia_titulo, midia_tipo FROM midia ORDER BY midia_id DESC"
        cursor.execute(query)
        items = cursor.fetchall()
        
        return jsonify(items)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if db and db.is_connected():
            db.close()

@app.route('/api/update_content/<int:media_id>', methods=['POST'])
def update_content(media_id):
    db = None
    try:
        db = get_db_connection()
        cursor = db.cursor()
        data = request.get_json()
        
        query = "UPDATE midia SET midia_titulo = %s WHERE midia_id = %s"
        cursor.execute(query, (data['title'], media_id))
        db.commit()
        
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if db and db.is_connected():
            db.close()

@app.route('/api/delete_content/<int:media_id>', methods=['DELETE'])
def delete_content(media_id):
    db = None
    try:
        db = get_db_connection()
        cursor = db.cursor()
        
        # You might want to delete from other tables as well (e.g., midia_players)
        cursor.execute("DELETE FROM midia WHERE midia_id = %s", (media_id,))
        db.commit()
        
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if db and db.is_connected():
            db.close()

@app.route('/api/bulk_delete_content', methods=['POST'])
def bulk_delete_content():
    db = None
    try:
        db = get_db_connection()
        cursor = db.cursor()
        data = request.get_json()
        content_type = data['type']
        
        query = "DELETE FROM midia WHERE midia_tipo = %s"
        cursor.execute(query, (content_type,))
        db.commit()
        
        return jsonify({'success': True, 'deleted_count': cursor.rowcount})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if db and db.is_connected():
            db.close()

@app.route('/api/list_duplicates', methods=['GET'])
def list_duplicates():
    request_id = str(uuid.uuid4())
    try:
        app.logger.info(f"[{request_id}] Listando duplicatas")
        duplicates = find_duplicates()
        return jsonify({'results': duplicates})
    except Exception as e:
        app.logger.error(f"[{request_id}] Erro ao listar duplicatas: {str(e)}", exc_info=True)
        return jsonify({'results': {'error': [{'message': f"Erro ao listar duplicatas: {str(e)}"}]}}), 500

@app.route('/api/delete_duplicates', methods=['POST'])
def delete_duplicates():
    request_id = str(uuid.uuid4())
    try:
        app.logger.info(f"[{request_id}] Iniciando exclusão de duplicatas")
        deleted_count = remove_duplicates()
        duplicates = find_duplicates()
        return jsonify({
            'results': {
                'success': True,
                'message': f"{deleted_count} duplicatas removidas",
                'total_duplicates': len(duplicates),
                'deleted_count': deleted_count,
                'remaining_duplicates': duplicates
            }
        })
    except Exception as e:
        app.logger.error(f"[{request_id}] Erro ao excluir duplicatas: {str(e)}", exc_info=True)
        return jsonify({'results': {'error': [{'message': f"Erro ao excluir duplicatas: {str(e)}"}]}}), 500
        
        
@app.route('/api/process_files', methods=['POST'])
def process_files():
    db = None
    request_id = str(uuid.uuid4())
    try:
        app.logger.info(f"[{request_id}] Iniciando processamento de arquivos")
        data = request.get_json()
        filenames = data.get('filenames', [])
        if not filenames:
            app.logger.error(f"[{request_id}] Nenhum arquivo especificado")
            return jsonify({'results': {'error': [{'message': 'Nenhum arquivo especificado'}]}}), 400

        results = {'success': [], 'error': [], 'processed_files': []}
        db = get_db_connection()

        for filename in filenames:
            m3u_path = os.path.join(M3U_DIR, filename)
            if not os.path.exists(m3u_path):
                app.logger.error(f"[{request_id}] Arquivo não encontrado: {m3u_path}")
                results['error'].append({'filename': filename, 'message': 'Arquivo não encontrado'})
                continue

            progress_file = os.path.join(M3U_DIR, f"{filename}.progress.json")
            chunk_size = 100

            total_urls = 0
            with open(m3u_path, 'r', encoding='utf-8') as f:
                lines = enumerate(f)
                for i, line in lines:
                    if line.strip().startswith('#EXTINF:'):
                        try:
                            next_line = next(lines)[1]
                            if next_line.strip().startswith('http'):
                                total_urls += 1
                        except StopIteration:
                            break

            if os.path.exists(progress_file):
                with open(progress_file, 'r') as f:
                    progress_data = json.load(f)
            else:
                progress_data = {'processed_urls': 0, 'total_urls': total_urls, 'results': {'success': [], 'exists': [], 'error': []}}

            start_url = progress_data['processed_urls']
            end_url = min(start_url + chunk_size, total_urls)
            chunk_results = {'success': [], 'exists': [], 'error': []}

            db.start_transaction()
            try:
                with open(m3u_path, 'r', encoding='utf-8') as f:
                    lines = enumerate(f)
                    current_url_idx = 0
                    for i, line in lines:
                        if line.strip().startswith('#EXTINF:'):
                            try:
                                next_line = next(lines)[1]
                                if next_line.strip().startswith('http'):
                                    current_url_idx += 1
                                    if start_url <= current_url_idx - 1 < end_url:
                                        extinf_line = line.strip()
                                        url_line = next_line.strip()
                                        match = re.match(r'#EXTINF:-?\d+(?:.*?(tvg-id="([^"]*)"))?(?:.*?(tvg-name="([^"]*)"))?(?:.*?(tvg-logo="([^"]*)"))?(?:.*?(group-title="([^"]*)"))?.*,(.+)', extinf_line)
                                        if match:
                                            tvg_id = match.group(2) or ''
                                            tvg_name = match.group(4) or ''
                                            tvg_logo = match.group(6) or ''
                                            group_title = match.group(8) or 'Sem Grupo'
                                            channel_name = match.group(9).strip()
                                            result = add_content_to_db(db, tvg_id, tvg_name, tvg_logo, group_title, channel_name, url_line)
                                            chunk_results['success'].extend(result['success'])
                                            chunk_results['exists'].extend(result['exists'])
                                            chunk_results['error'].extend(result['error'])
                                        else:
                                            chunk_results['error'].append({'message': f'Formato #EXTINF inválido: {extinf_line}'})
                            except StopIteration:
                                break
                db.commit()
            except Exception as e:
                db.rollback()
                app.logger.error(f"[{request_id}] Erro ao processar {filename}: {str(e)}")
                results['error'].append({'filename': filename, 'message': str(e)})
                continue

            progress_data['processed_urls'] += len(chunk_results['success']) + len(chunk_results['exists'])
            progress_data['results']['success'].extend(chunk_results['success'])
            progress_data['results']['exists'].extend(chunk_results['exists'])
            progress_data['results']['error'].extend(chunk_results['error'])

            processed = progress_data['processed_urls']
            total = progress_data['total_urls']
            progress = (processed / total * 100) if total > 0 else 0

            with open(progress_file, 'w') as f:
                json.dump(progress_data, f, ensure_ascii=False, indent=4)

            if progress >= 100 and os.path.exists(progress_file):
                os.remove(progress_file)

            app.logger.info(f"[{request_id}] Arquivo {filename} processado: {processed}/{total} ({progress:.2f}%)")
            results['processed_files'].append({
                'filename': filename,
                'success': chunk_results['success'],
                'exists': chunk_results['exists'],
                'error': chunk_results['error'],
                'processed': processed,
                'total': total,
                'progress': progress
            })

        return jsonify({'results': results})
    except Exception as e:
        app.logger.error(f"[{request_id}] Erro geral ao processar arquivos: {str(e)}", exc_info=True)
        return jsonify({'results': {'error': [{'message': f"Erro ao processar arquivos: {str(e)}"}]}}), 500
    finally:
        if db and db.is_connected():
            db.close()


if __name__ == '__main__':
    app.logger.info("Inicializando aplicação")
    db = get_db_connection()
    if db:
        init_db(db)
        db.close()
    app.run(debug=True)
