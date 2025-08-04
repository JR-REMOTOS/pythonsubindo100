import logging
from database import get_db_connection

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def remove_duplicates():
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

        deleted_count = 0
        db.start_transaction()
        try:
            for dup in duplicates:
                midia_titulo = dup['midia_titulo']
                midia_tipo = dup['midia_tipo']
                player_url = dup['player_url']

                cursor.execute("""
                    SELECT mp.player_id, mp.player_midia_id
                    FROM midia m
                    JOIN midia_players mp ON m.midia_id = mp.player_midia_id
                    WHERE m.midia_titulo = %s AND m.midia_tipo = %s AND mp.player_url = %s
                    ORDER BY mp.player_id ASC
                """, (midia_titulo, midia_tipo, player_url))
                players = cursor.fetchall()

                if len(players) > 1:
                    keep_player_id = players[0]['player_id']
                    for player in players[1:]:
                        cursor.execute("DELETE FROM midia_players WHERE player_id = %s", (player['player_id'],))
                        deleted_count += cursor.rowcount

                    cursor.execute("""
                        SELECT COUNT(*) as player_count
                        FROM midia_players
                        WHERE player_midia_id = %s
                    """, (players[0]['player_midia_id'],))
                    player_count = cursor.fetchone()['player_count']

                    if player_count == 0:
                        cursor.execute("DELETE FROM midia_episodios WHERE episodio_midia_id = %s", (players[0]['player_midia_id'],))
                        cursor.execute("DELETE FROM midia_temporadas WHERE temporada_midia_id = %s", (players[0]['player_midia_id'],))
                        cursor.execute("DELETE FROM midia WHERE midia_id = %s", (players[0]['player_midia_id'],))

            db.commit()
            return deleted_count
        except:
            db.rollback()
            raise
    except Exception as e:
        logging.error(f"Erro ao remover duplicatas: {str(e)}")
        raise
    finally:
        if cursor:
            cursor.close()
        if db and db.is_connected():
            db.close()