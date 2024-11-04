#region Imports
import pandas as pd
import sqlite3
import os
import re
import argparse
from datetime import datetime
# endregion

# region Configurazione
database = 'inventario.db'  # Nome del database SQLite
dir_ts_file_by_date = 'db_files'  # Directory contenente i file Excel (nella root dello script)
dir_odin_file = 'db_odin'

parser = argparse.ArgumentParser(description="Script di importazione e confronto inventario.")
parser.add_argument('-r', '--reset', action='store_true', help="Resetta il database eliminando i dati esistenti.")
args = parser.parse_args()

# conn = None
# cursor = None
# endregion

# region Functions
def init_db():
    # Se il file non esiste, crea una connessione che genererà il database, query per creare la tabella ts_by_date
    print("Init DB...")
    create_table_query = """
        CREATE TABLE IF NOT EXISTS ts_by_date (
            sku TEXT NOT NULL,
            data DATE NOT NULL,
            qta INTEGER NOT NULL,
            dep TEXT NOT NULL,
            PRIMARY KEY (sku, data, dep)
        );
        """
    cursor.execute(create_table_query)
    conn.commit()

    create_table_query = """
        CREATE TABLE IF NOT EXISTS odin_by_date (
            sku TEXT NOT NULL,
            data DATE NOT NULL,
            qta INTEGER NOT NULL,
            sez INTEGER NOT NULL,
            dep TEXT NOT NULL,
            sede TEXT NOT NULL
            PRIMARY KEY (sku, data, sez, dep, sede)
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    print("Database e tabella 'ts_by_date' creati con successo.")

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
    if table == 'ts':
        table_name = 'ts_by_date'  # Nome della tabella per i dati giornalieri

    elif table == 'odin':
        table_name = 'odin_by_date'

    df.to_sql(table_name, conn, if_exists='append', index=False)
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

    df=df[[list(required_columns)]]

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
# Connessione al database SQLite
conn = sqlite3.connect(database)
cursor = conn.cursor()

if not os.path.exists(database) or args.reset:
    init_db()

# Iterazione su tutti i file Excel nelle directory
# TS
for filename in os.listdir(dir_ts_file_by_date):
    if filename.endswith('.xlsx'):
        file_path = os.path.join(dir_ts_file_by_date, filename)
        import_ts_excel_to_sqlite(file_path)

# Odin
for filename in os.listdir(dir_odin_file):
    if filename.endswith('.xlsx'):
        file_path = os.path.join(dir_odin_file, filename)
        import_ts_excel_to_sqlite(file_path)


conn.close()
print("Importazione e confronto completati.")
# endregion