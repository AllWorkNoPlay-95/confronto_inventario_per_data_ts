#region Imports
import pandas as pd
import sqlite3
import mariadb
import os
from tqdm import tqdm
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
parser.add_argument('-v', '--verbose', action='store_true', help="Aumenta verbosità.")
parser.add_argument('--skip-ts', action='store_true', help="Salta importazione file TS.")
parser.add_argument('--skip-odin', action='store_true', help="Salta importazione Odin.")
args = parser.parse_args()
load_dotenv()  # Carica i segreti dall'.env


# conn = None
# cursor = None
# endregion

# region Connessioni
def init_app_db():
    global conn_app
    global cursor_app
    # Se il file non esiste, crea una connessione che genererà il database, query per creare la tabella ts_by_date
    if args.verbose:
        print("Init App DB...")

    try:
        create_table_query = """
            CREATE TABLE IF NOT EXISTS ts_by_date (
                sku TEXT NOT NULL,
                data DATE NOT NULL,
                qta INTEGER NOT NULL,
                dep TEXT NOT NULL,
                UNIQUE (sku, data, dep)
            );
            """
        cursor_app.execute(create_table_query)
        conn_app.commit()
        if args.verbose:
            print("Tabella ts_by_date creata.")

        create_table_query = """
            CREATE TABLE IF NOT EXISTS odin_by_date (
                sku TEXT NOT NULL,
                qta INTEGER NOT NULL,
                luogo TEXT NOT NULL,
                sez INTEGER NOT NULL,
                sede TEXT NOT NULL,
                data DATE NOT NULL,
                ultima_modifica DATE NOT NULL,
                note TEXT,
                username TEXT NOT NULL,
                UNIQUE (sku, sez, sede, luogo, username)
        );
        """
        cursor_app.execute(create_table_query)
        conn_app.commit()
        if args.verbose:
            print("Tabella odin_by_date creata.")

        create_table_query = """
                    CREATE TABLE IF NOT EXISTS imported_ts_files (
                        nome TEXT NOT NULL,
                        data TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE (nome)
                    );
                    """
        cursor_app.execute(create_table_query)
        conn_app.commit()
        if args.verbose:
            print("Tabella imported_ts_files creata.")

    except sqlite3.Error as e:
        print(e)
        exit(1)
    print("Database App creato. (SQLite)")


def connect_db_odin():
    tunnel = None
    conn = None

    try:
        tunnel = SSHTunnelForwarder(
            (os.getenv("ODIN_SSH_HOST"), int(os.getenv('ODIN_SSH_PORT'))),
            ssh_username=os.getenv('ODIN_SSH_USERNAME'),
            ssh_password=os.getenv('ODIN_SSH_PW'),
            remote_bind_address=(os.getenv('ODIN_DB_HOST'), int(os.getenv('ODIN_DB_PORT'))),
            local_bind_address=('127.0.0.1', 3308)
        )
        tunnel.start()
        if args.verbose:
            print("Tunnel SSH verso Odin creato con successo.")
    except Exception as e:
        print(e)
        exit(1)

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


# end region

# region Queries
def get_odin_inventario_completo_total_rows():
    if args.verbose:
        print("Conto righe inventario Odin...")
    cursor_odin.execute(
        "SELECT COUNT(*) FROM inventario_completo ic LEFT JOIN prodotti p ON ic.id_prod = p.id LEFT JOIN sedi s ON s.id = ic.id_sede;")
    return cursor_odin.fetchone()[0]


def get_imported_ts_files():
    global cursor_app
    query = """
    SELECT nome FROM imported_ts_files;
    """
    cursor_app.execute(query)
    result = cursor_app.fetchall()
    return [x[0] for x in result]


def insert_imported_ts_file(name):
    global cursor_app
    query = """
    INSERT INTO imported_ts_files (nome) VALUES (?);
    """
    cursor_app.execute(query, (name,))
    conn_app.commit()


def get_odin_inventario_completo_as_df(batchsize=5):
    global total_rows_odin
    offset = 0
    with tqdm(total=total_rows_odin, desc="Carico dati Odin...", unit="righe") as pbar:
        while offset < total_rows_odin:
            query = f"""
            SELECT
            IFNULL(cod,old_cod) AS sku,
            qta,
            luogo,
            sezione AS sez,
            s.nome AS sede,
            ic.data_creazione AS `data`,
            ic.ultima_modifica AS ultima_modifica,
            ic.note,
            u.username
            FROM inventario_completo ic 
            LEFT JOIN prodotti p ON ic.id_prod = p.id
            LEFT JOIN sedi s ON s.id = ic.id_sede
            LEFT JOIN users u ON u.id = ic.id_user
            LIMIT {batchsize} OFFSET {offset};
            """
            cursor_odin.execute(query)
            result = cursor_odin.fetchall()
            if not result:
                break
            result = pd.DataFrame.from_records(result,
                                               columns=["sku", "qta", "luogo", "sez", "sede", "data", "ultima_modifica",
                                                        "note", "username"])
            yield result
            offset += batchsize
            pbar.update(len(result))


def import_df_in_ts_by_date(df):
    if args.verbose:
        print("Eseguo query importazione in ts_bt_date...")
    for _, row in tqdm(df.iterrows(), total=df.shape[0]):
        cursor_app.execute("""
        INSERT OR IGNORE INTO ts_by_date (sku, data, qta, dep)
        VALUES (?,?,?,?)
        """, (row['sku'], row['data'], row['qta'], row['dep']))
    conn_app.commit()


def import_df_in_odin_by_date(df):
    for _, row in tqdm(df.iterrows(), total=df.shape[0], desc="Eseguo query importazione in odin_by_date...",
                       unit="righe"):
        row['data'] = row['data'].strftime('%Y-%m-%d %H:%M:%S')
        row['ultima_modifica'] = row['ultima_modifica'].strftime('%Y-%m-%d %H:%M:%S')
        cursor_app.execute("""
        INSERT  OR IGNORE INTO odin_by_date (sku, qta, luogo, sez, sede, data, ultima_modifica, note, username)
        VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            row['sku'],
            row['qta'],
            row['luogo'],
            row['sez'],
            row['sede'],
            row['data'],
            row['ultima_modifica'],
            row['note'],
            row['username']))
    conn_app.commit()


# end region

# region Files
# Funzione per estrarre la data dal nome del file
def extract_date_from_filename(name):
    match = re.search(r'(\d{2})-(\d{2})-(\d{4})', name)
    if match:
        day, month, year = match.groups()
        return datetime.strptime(f"{day}-{month}-{year}", "%d-%m-%Y").date()
    else:
        print(f"Errore: formato data non trovato nel nome del file '{name}'")
        return None


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

        df['data'] = data_date  # Assegna la data a tutte le righe

    elif table == 'odin':
        pass
    df = df[list(required_columns)]

    # Controlla stato di salute del DataFrame e prova a correggerlo
    if df.isnull().any().any():
        if args.verbose:
            print("Attenzione, queste righe NON sono valide:")
            print(df[df.isnull().any(axis=1)])
        df = df.dropna()

    if not required_columns.issubset(df.columns):
        print("Errore: Colonne mancanti nel file {file}")
        return

    return df


# end region
# region Esecuzione
# Connessione al database SQLite (DB app)
if args.reset:
    try:
        os.remove(database)
    except Exception as e:
        print(e)
        exit(1)

conn_app = sqlite3.connect(database)
cursor_app = conn_app.cursor()

# Connessione al database remoto odin MariaDB
tunnel_odin, conn_odin = connect_db_odin()
cursor_odin = conn_odin.cursor()

# Cache
total_rows_odin = 0
imported_ts_files = []

if not os.path.exists(database) or args.reset:
    init_app_db()

# Iterazione su tutti i file Excel nelle directory
# TS
if not args.skip_ts:
    if args.verbose:
        print(f"Importo file excel da {dir_ts_file_by_date}")
    files = os.listdir(dir_ts_file_by_date)
    imported_ts_files = get_imported_ts_files()

    for i, filename in enumerate(files):
        print("Importo file {}/{}".format(i + 1, len(files)))
        if filename.endswith('.xlsx'):
            if filename in imported_ts_files:
                if args.verbose:
                    print("File {} saltato.".format(filename))
                continue
            file_path = os.path.join(dir_ts_file_by_date, filename)
            import_df_in_ts_by_date(get_xlsx_as_df(file_path, 'ts'))
            insert_imported_ts_file(filename)
    if args.verbose:
        print("Dati importati in ts_by_date.")
else:
    if args.verbose:
        print("Salto importazione file TS. (--skip-ts)")

# Odin
if not args.skip_odin:
    total_rows_odin = get_odin_inventario_completo_total_rows()
    odin_df = pd.DataFrame()
    for batch in get_odin_inventario_completo_as_df(3):
        odin_df = pd.concat([odin_df, batch], ignore_index=True)
    import_df_in_odin_by_date(odin_df)

# endregion
# region Uscita
conn_app.close()
tunnel_odin.close()
conn_odin.close()
print("Importazione e confronto completati.")
# endregion
