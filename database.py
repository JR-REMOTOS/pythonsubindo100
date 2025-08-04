import mysql.connector
from mysql.connector import Error

def get_db_connection():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='popflix1',
            user='popflix1',
            password='winshester22',
            connection_timeout=30
        )
        return connection
    except Error as e:
        raise Exception(f"Erro ao conectar ao banco de dados: {e}")

def init_db(connection):
    cursor = connection.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS categoria (
            categoria_id INT AUTO_INCREMENT PRIMARY KEY,
            categoria_titulo VARCHAR(255) NOT NULL,
            categoria_descricao TEXT,
            categoria_diretorio VARCHAR(255) NOT NULL,
            categoria_image VARCHAR(36),
            categoria_para VARCHAR(20) NOT NULL DEFAULT 'midia'
        ) CHARACTER SET utf8 COLLATE utf8_general_ci
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS midia (
            midia_id INT AUTO_INCREMENT PRIMARY KEY,
            midia_titulo VARCHAR(255),
            midia_image VARCHAR(255),
            midia_background VARCHAR(255),
            midia_sinopse TEXT,
            midia_categoria INT,
            midia_tipo VARCHAR(50),
            midia_diretorio VARCHAR(255),
            midia_visualizacoes INT DEFAULT 0,
            FOREIGN KEY (midia_categoria) REFERENCES categoria(categoria_id)
        ) CHARACTER SET utf8 COLLATE utf8_general_ci
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS midia_temporadas (
            temporada_id INT AUTO_INCREMENT PRIMARY KEY,
            temporada_midia_id INT,
            temporada_titulo VARCHAR(255),
            temporada_diretorio VARCHAR(255),
            FOREIGN KEY (temporada_midia_id) REFERENCES midia(midia_id)
        ) CHARACTER SET utf8 COLLATE utf8_general_ci
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS midia_episodios (
            episodio_id INT AUTO_INCREMENT PRIMARY KEY,
            episodio_midia_id INT,
            episodio_temporada_id INT,
            episodio_titulo VARCHAR(255),
            episodio_diretorio VARCHAR(255),
            episodio_numero INT,
            FOREIGN KEY (episodio_midia_id) REFERENCES midia(midia_id),
            FOREIGN KEY (episodio_temporada_id) REFERENCES midia_temporadas(temporada_id)
        ) CHARACTER SET utf8 COLLATE utf8_general_ci
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS midia_players (
            player_id INT AUTO_INCREMENT PRIMARY KEY,
            player_midia_id INT,
            player_temporada_id INT,
            player_episodio_id INT,
            player_titulo VARCHAR(255),
            player_url VARCHAR(255),
            player_tipo VARCHAR(50),
            player_audio VARCHAR(50),
            player_acesso VARCHAR(50),
            FOREIGN KEY (player_midia_id) REFERENCES midia(midia_id),
            FOREIGN KEY (player_temporada_id) REFERENCES midia_temporadas(temporada_id),
            FOREIGN KEY (player_episodio_id) REFERENCES midia_episodios(episodio_id)
        ) CHARACTER SET utf8 COLLATE utf8_general_ci
    """)
    cursor.execute("""
        INSERT IGNORE INTO categoria (categoria_id, categoria_titulo, categoria_diretorio, categoria_para) VALUES
        (1, 'Filmes', 'filmes', 'midia'),
        (17, 'Séries', 'series', 'midia'),
        (18, 'Novelas', 'novelas', 'midia'),
        (19, 'Anime', 'anime', 'midia'),
        (20, 'Dorama', 'dorama', 'midia'),
        (21, 'Infantil', 'infantil', 'midia'),
        (38, 'Canais', 'canais', 'midia')
    """)
    connection.commit()
    cursor.close()