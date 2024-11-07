# Inventory Comparison Script

### Descrizione
Questo script Python gestisce l'importazione, l'elaborazione e il confronto di inventari provenienti da diverse fonti. L'obiettivo è fornire una panoramica consolidata delle giacenze a una certa data, rilevando eventuali discrepanze tra i dati rilevati e quelli attesi.

L'applicazione è progettata per:
- Importare dati di inventario da file Excel e database remoti tramite SSH.
- Integrare e sincronizzare le informazioni in un database SQLite locale.
- Generare report dettagliati, individuando e visualizzando in modo chiaro le discrepanze rilevate tra fonti.
- Esportare i risultati in Excel con una formattazione leggibile e professionale.

### Funzionalità principali

1. **Connessioni Database**:
   - Collega un database SQLite locale (`inventario.db`) per memorizzare i dati di inventario temporaneamente.
   - Collega un database MariaDB remoto tramite tunnel SSH, consentendo un accesso sicuro ai dati dell'inventario principale.
   - Crea dinamicamente tabelle SQLite per memorizzare dati specifici, come i file importati, i metadati dei prodotti e le giacenze rilevate, assicurando un’organizzazione strutturata.

2. **Importazione Dati da File Excel**:
   - Legge e importa i file Excel (con estensione `.xlsx`) presenti nella directory specificata (`db_files`).
   - Estrae automaticamente la data dal nome del file per includerla nei dati di importazione.
   - Effettua controlli preliminari sulla presenza di colonne essenziali e rimuove eventuali righe incomplete per garantire la consistenza dei dati.

3. **Elaborazione e Confronto**:
   - Confronta i dati rilevati (`odin_by_date`) con le giacenze attese (`ts_by_date`) per rilevare discrepanze.
   - Utilizza query SQL ottimizzate con `LEFT JOIN` e `CASE` per calcolare differenze tra quantità rilevate e giacenze attese per SKU, sede e data.
   - Mostra un report tabellare delle discrepanze nella console e offre la possibilità di esportare i risultati in un file Excel.

4. **Export in Excel**:
   - Al termine del confronto, l'utente può scegliere di esportare i risultati in un file Excel ben formattato, con formattazioni di colonna e intestazione per migliorare la leggibilità.

5. **Interfaccia Utente (CLI)**:
   - Argomenti della riga di comando per configurare facilmente le opzioni di esecuzione:
     - `-r`, `--reset`: Elimina il database esistente e ricrea le tabelle.
     - `-v`, `--verbose`: Aumenta la verbosità, visualizzando messaggi di log dettagliati durante l'esecuzione.
     - `--skip-ts`, `--skip-odin`, `--skip-prod-meta`: Salta specifiche fasi di importazione, rendendo lo script più flessibile.

### Esecuzione

#### Requisiti
- **Python 3.7+**
- **Librerie Python**: Specifiche librerie Python per l'elaborazione dei dati e la connessione ai database.
  ```bash
  pip install pandas mariadb xlsxwriter tabulate tqdm python-dotenv sshtunnel
  ```

#### Esecuzione dello Script
Esegui lo script dalla riga di comando:

```bash
python inventory_comparison.py -v --reset
```

Opzioni disponibili:
- `-r`, `--reset`: Resetta il database locale.
- `-v`, `--verbose`: Aumenta la verbosità per il debug.
- `--skip-ts`: Salta l'importazione dei file `TS`.
- `--skip-odin`: Salta l'importazione da `Odin`.
- `--skip-prod-meta`: Salta l'importazione dei metadati dei prodotti.

#### Output
- Il confronto delle giacenze viene stampato in formato tabellare nella console, mostrando SKU, descrizione, quantità rilevata, quantità attesa e discrepanza.
- L'utente può scegliere di esportare i risultati in un file Excel (`export/Confronto Inventario del GG-MM-YYYY HH-MM.xlsx`).

### Struttura dello Script

- **Connessioni**:
  - `init_app_db()`: Inizializza il database locale con tabelle strutturate.
  - `connect_db_odin()`: Stabilisce un tunnel SSH sicuro e si connette a un database MariaDB remoto.
- **Importazione e Sincronizzazione**:
  - `get_xlsx_as_df()`: Converte i file Excel in `DataFrame` e li prepara per l'importazione.
  - `import_df_in_ts_by_date()` e `import_df_in_odin_by_date()`: Funzioni per l'importazione dei dati nei database locali.
  - `transfer_missing_products_meta_to_local_db()`: Sincronizza i metadati dei prodotti da `Odin` a SQLite.
- **Analisi delle Discrepanze**:
  - `calc_discrepancy()`: Calcola e visualizza le discrepanze tra giacenze attese e quantità rilevate.
  - `export_as_excel()`: Esporta i risultati delle discrepanze in Excel, con formattazione personalizzata.

### (Per i recruiter) Valutazione delle competenze

Questo progetto dimostra competenze avanzate nelle seguenti aree:
- **Gestione Database**: Creazione, connessione e gestione di database SQLite e MariaDB, incluse operazioni avanzate tramite `JOIN` e sottoquery per ottimizzare l'accesso ai dati.
- **Elaborazione Dati**: Importazione, trasformazione e pulizia dei dati da Excel tramite `pandas`, con metodi di controllo e correzione della qualità dei dati.
- **Integrazione e Sicurezza**: Connessione sicura ai database remoti via SSH usando `sshtunnel`, insieme a una gestione sicura delle credenziali tramite `.env`.
- **Interfaccia Utente CLI**: Implementazione di un'interfaccia a riga di comando completa e flessibile, rendendo lo script utilizzabile in contesti differenti.
- **Visualizzazione e Esportazione**: Visualizzazione tabellare delle discrepanze tramite `tabulate` e formattazione personalizzata per l’esportazione in Excel.

### Possibili Estensioni
- **Gestione degli Errori**: Ulteriori ottimizzazioni nella gestione delle eccezioni per migliorare la resilienza.
- **Logging**: Aggiunta di un sistema di logging per migliorare il monitoraggio e la risoluzione dei problemi.
- **Automatizzazione**: Pianificazione dello script per eseguire importazioni e confronti periodici tramite un task scheduler.

### Conclusione
Questo script è un esempio di applicazione che utilizza un'architettura modulare per gestire dati complessi in modo sicuro e versatile. La struttura, l’uso delle librerie e l’attenzione alla qualità del codice sono segni di una buona comprensione delle best practices di programmazione Python per il data engineering e la business intelligence.