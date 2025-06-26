# index_documents.py  
import sqlite3  
import os  

# Ensure the database directory exists  
os.makedirs("database", exist_ok=True)  

# Connect to SQLite database (creates file if missing)  
conn = sqlite3.connect("database/documents.db")  
cursor = conn.cursor()  

# Create table  
cursor.execute('''CREATE TABLE IF NOT EXISTS documents (  
    id INTEGER PRIMARY KEY AUTOINCREMENT,  
    title TEXT,  
    document_id TEXT,  
    organization TEXT,  
    publication_date DATE,  
    abstract TEXT,  
    file_path TEXT,  
    file_type TEXT,  
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  
)''')  

# Create indexes for fast search  
cursor.execute('CREATE INDEX IF NOT EXISTS idx_title ON documents(title)')  
cursor.execute('CREATE INDEX IF NOT EXISTS idx_abstract ON documents(abstract)')  
cursor.execute('CREATE INDEX IF NOT EXISTS idx_document_id ON documents(document_id)')  

conn.commit()  
conn.close()  