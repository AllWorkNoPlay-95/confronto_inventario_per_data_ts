import pandas as pd
import sqlite3
import os
import re
import argparse
from datetime import datetime

# Configurazione
database = 'inventario.db'  # Nome del database SQLite
table_name = 'ts_by_date'  # Nome della tabella per i dati giornalieri
directory = 'db_files'  # Directory contenente i file Excel (nella root dello script)

### Parser
# Configura argparse per riconoscere il parametro -r / --reset
parser = argparse.ArgumentParser(description="Script di importazione e confronto inventario.")
parser.add_argument('-r', '--reset', action='store_true', help="Resetta il database eliminando i dati esistenti.")

# Parsing degli argomenti
args = parser.parse_args()
###

conn = sqlite3.connect(database)
cursor = conn.cursor()

def reset_db():
    # Se il file non esiste, crea una connessione che genererà il database
    # Query per creare la tabella ts_by_date
    print("Reset DB...")
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
    print("Database e tabella 'ts_by_date' creati con successo.")

# Connessione al database SQLite
if not os.path.exists(database) or args.reset:
    reset_db()
else:
    print(f"{database} già esistente, proseguo.")

# Funzione per estrarre la data dal nome del file
def extract_date_from_filename(filename):
    match = re.search(r'(\d{2})-(\d{2})-(\d{4})', filename)
    if match:
        day, month, year = match.groups()
        return datetime.strptime(f"{day}-{month}-{year}", "%d-%m-%Y").date()
    else:
        print(f"Errore: formato data non trovato nel nome del file '{filename}'")
        return None


# Funzione per importare i dati da Excel a SQLite
def import_excel_to_sqlite(file_path):
    required_columns = {"sku", "qta", "dep", "data"}
    # Lettura del file Excel
    df = pd.read_excel(file_path)
    # Rinomina delle colonne in base alla mappatura richiesta
    df.rename(columns={
        "Codice articolo": "sku",
        "Giac.att.1": "qta",
        "Dep": "dep"
    }, inplace=True)


    # Estrazione della data dal nome del file
    data_date = extract_date_from_filename(os.path.basename(file_path))
    if data_date is None:
        return  # Salta il file se la data non è valida

    df['data'] = data_date # Assegna la data a tutte le righe
    df=df[["sku", "qta", "dep", "data"]]

    #Controlla stato di salute del DataFrame e prova a correggerlo
    if df.isnull().any().any():
        print("Attenzione, queste righe NON sono valide:")
        print(df[df.isnull().any(axis=1)])
        df = df.dropna()

    if not required_columns.issubset(df.columns):
        print(f"Errore: Colonne mancanti nel file {file_path}")
        return

    # Inserimento dei dati nel database
    df.to_sql(table_name, conn, if_exists='append', index=False)
    print(f"Dati importati da {file_path}.")


# Iterazione su tutti i file Excel nella directory db_files
for filename in os.listdir(directory):
    if filename.endswith('.xlsx') or filename.endswith('.xls'):
        file_path = os.path.join(directory, filename)
        import_excel_to_sqlite(file_path)


# Chiusura della connessione
conn.close()
print("Importazione e confronto completati.")
