#region Imports
import pandas as pd
import sqlite3
import mariadb
import os
import dotenv
from dotenv import load_dotenv
import re
import argparse
from datetime import datetime
from sshtunnel import SSHTunnelForwarder
# endregion

# region Configurazione
database = 'inventario.db'  # Nome del database SQLite
dir_ts_file_by_date = 'db_files'  # Directory contenente i file Excel (nella root dello script)
dir_odin_file = 'db_odin'

parser = argparse.ArgumentParser(description="Script di importazione e confronto inventario.")
parser.add_argument('-r', '--reset', action='store_true', help="Resetta il database eliminando i dati esistenti.")
args = parser.parse_args()
load_dotenv() # Carica i segreti dall'.env

# conn = None
# cursor = None
# endregion

# region Functions
def init_app_db():
    global conn_app
    global cursor_app
    # Se il file non esiste, crea una connessione che genererà il database, query per creare la tabella ts_by_date
    print("Init App DB...")
    create_table_query = """
        CREATE TABLE IF NOT EXISTS ts_by_date (
            sku TEXT NOT NULL,
            data DATE NOT NULL,
            qta INTEGER NOT NULL,
            dep TEXT NOT NULL,
            PRIMARY KEY (sku, data, dep)
        );
        """
    cursor_app.execute(create_table_query)
    conn_app.commit()
    print("Tabella ts_by_date creata.")

    create_table_query = """
        CREATE TABLE IF NOT EXISTS odin_by_date (
            sku TEXT NOT NULL,
            data DATE NOT NULL,
            qta INTEGER NOT NULL,
            sez INTEGER NOT NULL,
            dep TEXT NOT NULL,
            sede TEXT NOT NULL,
            PRIMARY KEY (sku, data, sez, dep, sede)
    );
    """
    cursor_app.execute(create_table_query)
    conn_app.commit()

    print("Database App creato con successo. (SQLite)")

def connect_db_odin():
    tunnel = None
    conn = None

    try:
        tunnel = SSHTunnelForwarder(
            (os.getenv("ODIN_SSH_HOST"),int(os.getenv('ODIN_SSH_PORT'))),
            ssh_username=os.getenv('ODIN_SSH_USERNAME'),
            ssh_password=os.getenv('ODIN_SSH_PW'),
            remote_bind_address=(os.getenv('ODIN_DB_HOST'), int(os.getenv('ODIN_DB_PORT'))),
            local_bind_address=('127.0.0.1', 3308)
        )
        tunnel.start()
        print("Tunnel SSH verso Odin creato con successo.")
    except Exception as e:
        print(e)
        exit(1)
    print(tunnel.local_bind_host)
    try:
        conn = mariadb.connect(
            user=os.getenv('ODIN_DB_NAME'),
            password=os.getenv('ODIN_DB_PW'),
            host=tunnel.local_bind_host,
            port=tunnel.local_bind_port,
            database=os.getenv('ODIN_DB_NAME')
        )
    except mariadb.Error as e:
        print(e)
        exit(1)
    return tunnel, conn

# Funzione per estrarre la data dal nome del file
def extract_date_from_filename(name):
    match = re.search(r'(\d{2})-(\d{2})-(\d{4})', name)
    if match:
        day, month, year = match.groups()
        return datetime.strptime(f"{day}-{month}-{year}", "%d-%m-%Y").date()
    else:
        print(f"Errore: formato data non trovato nel nome del file '{name}'")
        return None

# Inserimento dei dati nel database
def import_df_in_db(df, table):
    table_name = None
    if table == 'ts':
        table_name = 'ts_by_date'  # Nome della tabella per i dati giornalieri

    elif table == 'odin':
        table_name = 'odin_by_date'

    df.to_sql(table_name, conn_app, if_exists='append', index=False)
    print(f"Dati importati in {table_name}.")

# Funzione per estrarre i dati da excel
def get_xlsx_as_df(file, table):
    df = pd.read_excel(file)
    required_columns = {}

    if table == "ts":
        required_columns = {"sku", "qta", "dep", "data"}

        # Rinomina delle colonne in base alla mappatura richiesta
        df.rename(columns={
            "Codice articolo": "sku",
            "Giac.att.1": "qta",
            "Dep": "dep"
        }, inplace=True)

        # Estrazione della data dal nome del file
        data_date = extract_date_from_filename(os.path.basename(file))

        if data_date is None:
            return  # Salta il file se la data non è valida

        df['data'] = data_date # Assegna la data a tutte le righe

    elif table == 'odin':
        pass
    df=df[list(required_columns)]

    # Controlla stato di salute del DataFrame e prova a correggerlo
    if df.isnull().any().any():
        print("Attenzione, queste righe NON sono valide:")
        print(df[df.isnull().any(axis=1)])
        df = df.dropna()

    if not required_columns.issubset(df.columns):
        print(f"Errore: Colonne mancanti nel file {file}")
        return

    return df
# endregion

# region Esecuzione
# Connessione al database SQLite (DB app)
conn_app = sqlite3.connect(database)
cursor_app = conn_app.cursor()

# Connessione al database remoto odin MariaDB
tunnel_odin, conn_odin = connect_db_odin()
cursor_odin = conn_odin.cursor()


if not os.path.exists(database) or args.reset:
    init_app_db()

cursor_odin.execute('SELECT * FROM prodotti LIMIT 5;')
for row in cursor_odin:
    print(row)

# Iterazione su tutti i file Excel nelle directory
# TS
for filename in os.listdir(dir_ts_file_by_date):
    if filename.endswith('.xlsx'):
        file_path = os.path.join(dir_ts_file_by_date, filename)
        import_df_in_db(get_xlsx_as_df(file_path, 'ts'), 'ts')

# endregion
# region Uscita
conn_app.close()
tunnel_odin.close()
conn_odin.close()
print("Importazione e confronto completati.")
# endregion