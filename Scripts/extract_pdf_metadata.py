import os
import fitz  # PyMuPDF
import re
from datetime import datetime
import logging
import json
import sqlite3

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def extract_pdf_metadata(pdf_path):
    """Extract metadata and abstract from PDF files"""
    try:
        # Use PyMuPDF's fitz.open()
        doc = fitz.open(pdf_path)
        
        # Skip encrypted files
        if doc.is_encrypted:
            logging.warning(f"Encrypted PDF skipped: {pdf_path}")
            return None
            
        metadata = {
            "document_id": os.path.splitext(os.path.basename(pdf_path))[0],
            "file_path": pdf_path,
            "title": "",
            "abstract": "",
            "publication_date": "",
            "organization": "ISO"
        }
        
        # Extract metadata
        pdf_metadata = doc.metadata
        if pdf_metadata.get("title"):
            metadata["title"] = pdf_metadata["title"].strip()
            
        if pdf_metadata.get("creationDate"):
            try:
                # Convert PDF date format to ISO format
                if pdf_metadata["creationDate"].startswith("D:"):
                    metadata["publication_date"] = f"{pdf_metadata['creationDate'][2:6]}-{pdf_metadata['creationDate'][6:8]}-{pdf_metadata['creationDate'][8:10]}"
            except Exception as e:
                logging.warning(f"Date parsing failed: {str(e)}")
                
        # Extract first 20 pages to find abstract
        text = ""
        for page_num in range(min(20, len(doc))):
            text += doc[page_num].get_text()
        
        # Detect structured abstract
        paragraphs = [p.strip() for p in text.split("\n\n") if len(p.strip().split()) > 50]
        if paragraphs:
            for i, para in enumerate(paragraphs):
                if any(kw in para.lower() for kw in ["abstract", "summary", "scope"]):
                    metadata["abstract"] = para[:500]
                    break
            else:
                metadata["abstract"] = paragraphs[0][:500]  # First valid paragraph
        
        # Try to extract ISO number
        iso_match = re.search(r'(ISO|IEC)\s*(\d+)', text)
        if iso_match:
            metadata["document_id"] = iso_match.group(0)
            
        return metadata
        
    except Exception as e:
        logging.error(f"PDF extraction failed for {pdf_path}: {str(e)}")
        return None

def save_metadata(metadata):
    """Save metadata to JSON in domain/year structure"""
    raw_parts = os.path.normpath(metadata["file_path"]).split(os.sep)
    
    try:
        raw_index = raw_parts.index("raw")
        domain = raw_parts[raw_index + 1]
        year = raw_parts[raw_index + 2]
    except (ValueError, IndexError):
        logging.error(f"Invalid file path structure: {metadata['file_path']}")
        return None
    
    processed_dir = os.path.join("data", "processed", "pdf", domain, year)
    os.makedirs(processed_dir, exist_ok=True)
    
    output_path = os.path.join(
        processed_dir, 
        f"{os.path.splitext(os.path.basename(metadata['file_path']))[0]}.json"
    )
    
    try:
        with open(output_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        logging.info(f"Saved metadata: {output_path}")
        return output_path
    except Exception as e:
        logging.error(f"JSON save failed: {str(e)}")
        return None

def insert_into_sqlite(metadata):  
    """Insert metadata into SQLite database (without hashing)"""  
    import sqlite3
    conn = sqlite3.connect("database/documents.db")
    cursor = conn.cursor()  

    # Standardize date format  
    pub_date = metadata.get("publication_date", "")  
    if pub_date and len(pub_date) > 10:  
        pub_date = pub_date[:10]  

    try:  
        cursor.execute('''INSERT INTO documents 
                     (title, document_id, organization, publication_date, abstract, file_path, file_type)  
                     VALUES (?, ?, ?, ?, ?, ?, ?)''',  
                     (metadata.get("title", ""),  
                      metadata.get("document_id", ""),  
                      metadata.get("organization", "ISO"),  
                      pub_date,  
                      metadata.get("abstract", ""),  
                      metadata.get("file_path", ""),  
                      "pdf"))
        conn.commit()  
        logging.info(f"Inserted into SQLite: {metadata.get('document_id')}")  
    except Exception as e:  
        logging.error(f"SQLite insertion failed: {str(e)}")  
    finally:  
        conn.close()  

def process_pdf_files(raw_dir="data/raw"):
    """Process all PDFs in raw directory"""
    metadata_list = []
    
    # Create database if missing
    os.makedirs("database", exist_ok=True)
    conn = sqlite3.connect("database/documents.db")
    cursor = conn.cursor()
    
    # Create table if missing (no hashing)
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
    
    conn.commit()
    conn.close()
    
    # Process files
    for root, _, files in os.walk(raw_dir):
        for file in files:
            if file.lower().endswith('.pdf'):
                pdf_path = os.path.join(root, file)
                if metadata := extract_pdf_metadata(pdf_path):
                    metadata_list.append(metadata)
                    if save_path := save_metadata(metadata):
                        insert_into_sqlite(metadata)
    
    return metadata_list

if __name__ == "__main__":
    process_pdf_files()