Questo script importa in batch file Excel contenenti dati di giacenze giornaliere dal gestionale in un database SQLite, li archivia in una tabella "ts_by_date", e confronta le giacenze attuali (tramite la funzione "Inventario Completo di Odin") con un inventario storico.

Per ogni file:
- Estrae la data dal nome (formato GG-MM-AAAA) e assegna questa data alle righe importate.
- Carica i dati in una tabella con chiavi SKU, DATA, e DEP per evitare duplicati.
- Confronta le giacenze di una data specifica e deposito, evidenziando le differenze con l'inventario storico.

Questo confronto aiuta a rilevare discrepanze di inventario nel tempo.
