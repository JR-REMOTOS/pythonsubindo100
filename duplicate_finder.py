import logging
from database import get_db_connection

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def find_duplicates():
    db = None
    cursor = None
    try:
        db = get_db_connection()
        cursor = db.cursor(dictionary=True)

        cursor.execute("""
            SELECT m.midia_titulo, m.midia_tipo, mp.player_url, COUNT(*) as count
            FROM midia m
            JOIN midia_players mp ON m.midia_id = mp.player_midia_id
            GROUP BY m.midia_titulo, m.midia_tipo, mp.player_url
            HAVING COUNT(*) > 1
        """)
        duplicates = cursor.fetchall()
        return duplicates
    except Exception as e:
        logging.error(f"Erro ao buscar duplicatas: {str(e)}")
        raise
    finally:
        if cursor:
            cursor.close()
        if db and db.is_connected():
            db.close()

def remove_duplicates():
    db = None
    cursor = None
    try:
        db = get_db_connection()
        cursor = db.cursor()
        db.start_transaction()

        # Deletar duplicatas, mantendo a entrada com menor midia_id
        cursor.execute("""
            DELETE m1 FROM midia m1
            INNER JOIN midia m2
            WHERE m1.midia_id > m2.midia_id
            AND m1.midia_titulo = m2.midia_titulo
            AND m1.midia_tipo = m2.midia_tipo
            AND EXISTS (
                SELECT 1 FROM midia_players mp1
                JOIN midia_players mp2 ON mp1.player_url = mp2.player_url
                WHERE mp1.player_midia_id = m1.midia_id
                AND mp2.player_midia_id = m2.midia_id
            )
        """)
        deleted_count = cursor.rowcount

        # Limpar midia_players órfãos
        cursor.execute("""
            DELETE mp FROM midia_players mp
            LEFT JOIN midia m ON mp.player_midia_id = m.midia_id
            WHERE m.midia_id IS NULL
        """)

        db.commit()
        logging.info(f"Removidas {deleted_count} duplicatas")
        return deleted_count
    except Exception as e:
        db.rollback()
        logging.error(f"Erro ao remover duplicatas: {str(e)}")
        raise
    finally:
        if cursor:
            cursor.close()
        if db and db.is_connected():
            db.close()