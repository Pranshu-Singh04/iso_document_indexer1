import os
import sqlite3
import json
import logging
from datetime import datetime
import csv
import argparse
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def create_database():
    """Create SQLite database with basic schema"""
    conn = sqlite3.connect("database/documents.db")
    c = conn.cursor()
    
    # Create table without hashing
    c.execute('''CREATE TABLE IF NOT EXISTS documents (
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
    c.execute('CREATE INDEX IF NOT EXISTS idx_title ON documents(title)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_abstract ON documents(abstract)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_date ON documents(publication_date)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_domain ON documents(file_path)')
    
    conn.commit()
    return conn

def insert_metadata(conn, metadata, file_type):
    """Insert metadata into database without hashing"""
    cursor = conn.cursor()
    
    pub_date = metadata.get("publication_date", "")
    if pub_date and len(pub_date) > 10:
        pub_date = pub_date[:10]  # YYYY-MM-DD
    
    try:
        cursor.execute('''INSERT INTO documents 
                          (title, document_id, organization, publication_date, 
                           abstract, file_path, file_type)
                          VALUES (?, ?, ?, ?, ?, ?, ?)''',
                     (metadata.get("title", ""),
                      metadata.get("document_id", ""),
                      metadata.get("organization", "ISO"),
                      pub_date,
                      metadata.get("abstract", ""),
                      metadata.get("file_path", ""),
                      file_type))
        conn.commit()
        logging.info(f"Inserted: {metadata.get('document_id')}")
        return True
    except Exception as e:
        logging.error(f"DB insertion failed: {str(e)}")
        return False

def index_documents():
    """Index all metadata files into SQLite"""
    conn = create_database()
    
    # Process PDF metadata
    for root, _, files in os.walk("data/processed/pdf"):
        for file in files:
            if file.lower().endswith('.json'):
                json_path = os.path.join(root, file)
                with open(json_path, 'r') as f:
                    metadata = json.load(f)
                insert_metadata(conn, metadata, "pdf")
    
    # Process XML metadata
    for root, _, files in os.walk("data/processed/xml"):
        for file in files:
            if file.lower().endswith('.json'):
                json_path = os.path.join(root, file)
                with open(json_path, 'r') as f:
                    metadata = json.load(f)
                insert_metadata(conn, metadata, "xml")
    
    conn.close()
    logging.info("Indexing completed")

def search_documents(keyword=None, domain=None, file_type=None, start_year=None, end_year=None):
    """Search documents by keyword, domain, file type, and date range"""
    conn = sqlite3.connect("database/documents.db")
    cursor = conn.cursor()
    
    query = "SELECT * FROM documents WHERE 1=1"
    params = []
    
    # Keyword search
    if keyword:
        query += " AND (title LIKE ? OR abstract LIKE ?)"
        params.extend([f"%{keyword}%", f"%{keyword}%"])
    
    # Domain filter
    if domain:
        query += " AND file_path LIKE ?"
        params.append(f"%{domain}%")
    
    # File type filter
    if file_type:
        query += " AND file_type = ?"
        params.append(file_type)
    
    # Date range filter
    if start_year and re.match(r'\d{4}', start_year):
        query += " AND publication_date >= ?"
        params.append(f"{start_year}-01-01")
    
    if end_year and re.match(r'\d{4}', end_year):
        query += " AND publication_date <= ?"
        params.append(f"{end_year}-12-31")
    
    try:
        results = cursor.execute(query, params).fetchall()
        logging.info(f"Found {len(results)} matching documents")
        return results
    except Exception as e:
        logging.error(f"Search failed: {str(e)}")
        return []
    finally:
        conn.close()

def export_to_csv(results, filename="search_results.csv"):
    """Export search results to CSV"""
    output_path = os.path.join("data", filename)
    
    fieldnames = ["title", "document_id", "organization", "publication_date",
                 "abstract", "file_path", "file_type", "processed_at"]
    
    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in results:
            writer.writerow({
                "title": row[1],
                "document_id": row[2],
                "organization": row[3],
                "publication_date": row[4],
                "abstract": row[5],
                "file_path": row[6],
                "file_type": row[7],
                "processed_at": row[8]
            })
    
    logging.info(f"Exported {len(results)} results to {output_path}")
    return output_path

def main():
    parser = argparse.ArgumentParser(description='Document Indexer CLI')
    subparsers = parser.add_subparsers(dest='command', required=True)
    
    # Index command
    index_parser = subparsers.add_parser('index', help='Index all documents')
    
    # Search command
    search_parser = subparsers.add_parser('search', help='Search indexed documents')
    search_parser.add_argument("--keyword", "-k", help="Keyword to search in title/abstract")
    search_parser.add_argument("--domain", "-d", help="Filter by domain (e.g., public.resource.org)")
    search_parser.add_argument("--type", "-t", help="Filter by file type (pdf/xml)")
    search_parser.add_argument("--after", "-a", help="Filter by start year (e.g., 2020)")
    search_parser.add_argument("--before", "-b", help="Filter by end year (e.g., 2025)")
    search_parser.add_argument("--export", "-e", help="Export results to CSV file")
    
    args = parser.parse_args()
    
    if args.command == "index":
        index_documents()
    
    elif args.command == "search":
        # Validate input
        if not any([args.keyword, args.domain, args.type, args.after, args.before]):
            logging.warning("Please provide at least one search filter (--keyword, --domain, --type, --after, --before)")
            return
        
        # Run search
        results = search_documents(
            keyword=args.keyword,
            domain=args.domain,
            file_type=args.type,
            start_year=args.after,
            end_year=args.before
        )
        
        # Print results
        logging.info(f"Found {len(results)} matching documents:")
        for row in results:
            print(f"{row[1]} | {row[2]} | {row[4]} | {row[3]}")
        
        # Export if requested
        if args.export:
            export_to_csv(results, args.export)

if __name__ == "__main__":
    main()