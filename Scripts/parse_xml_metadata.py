import os
import xml.etree.ElementTree as ET
from datetime import datetime
import logging
import json
import sqlite3
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def detect_xml_schema(root):
    """Detect XML schema type based on namespace or root element"""
    if "http://purl.org/dc/elements/1.1/" in root.tag:
        return "dublin_core"
    elif "http://www.tei-c.org/ns/1.0" in root.tag:
        return "tei"
    elif "http://www.iso.org/ns" in root.tag:
        return "iso"
    else:
        return "unknown"

def parse_xml_metadata(xml_path):
    """Extract metadata from XML files"""
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        schema = detect_xml_schema(root)
        logging.info(f"Detected XML schema: {schema}")
        
        # Define namespace mappings
        ns = {
            'dc': 'http://purl.org/dc/elements/1.1/',
            'dcterms': 'http://purl.org/dc/terms/',
            'iso': 'http://www.iso.org/ns'
        }
        
        metadata = {
            "document_id": os.path.splitext(os.path.basename(xml_path))[0],
            "file_path": xml_path,
            "title": "",
            "abstract": "",
            "publication_date": "",
            "organization": "ISO",
            "file_type": "xml"
        }
        
        # Schema-specific parsing
        if schema == "dublin_core":
            title = root.find('.//dc:title', ns) or root.find('.//title')
            abstract = root.find('.//dcterms:abstract', ns) or root.find('.//abstract')
            date_node = root.find('.//dcterms:issued', ns) or root.find('.//date')
        
        elif schema == "tei":
            title = root.find('.//tei:titleStmt/tei:title', ns) or root.find('.//title')
            abstract = root.find('.//tei:profileDesc/tei:abstract', ns) or root.find('.//abstract')
            date_node = root.find('.//tei:publicationStmt/tei:date', ns) or root.find('.//date')
        
        elif schema == "iso":
            title = root.find('.//iso:title', ns) or root.find('.//title')
            abstract = root.find('.//iso:abstract', ns) or root.find('.//abstract')
            date_node = root.find('.//iso:publicationDate', ns) or root.find('.//date')
        
        else:
            # Fallback: Try basic parsing
            title = root.find('.//title')
            abstract = root.find('.//abstract')
            date_node = root.find('.//date')
        
        # Extract values
        if title is not None and title.text:
            metadata["title"] = title.text.strip()
        if abstract is not None and abstract.text:
            metadata["abstract"] = abstract.text.strip()[:500]
        if date_node is not None and date_node.text:
            try:
                metadata["publication_date"] = datetime.strptime(date_node.text[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
            except:
                metadata["publication_date"] = date_node.text[:10]
        
        return metadata
        
    except ET.ParseError as e:
        logging.error(f"XML parsing failed for {xml_path}: {str(e)}")
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
    
    processed_dir = os.path.join("data", "processed", "xml", domain, year)
    os.makedirs(processed_dir, exist_ok=True)
    
    output_path = os.path.join(
        processed_dir, 
        f"{os.path.splitext(os.path.basename(metadata['file_path']))[0]}.json"
    )
    
    try:
        with open(output_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        logging.info(f"Saved XML metadata: {output_path}")
        return output_path
    except Exception as e:
        logging.error(f"JSON save failed: {str(e)}")
        return None

def insert_into_sqlite(metadata):  
    """Insert metadata into SQLite database"""  
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
                      metadata.get("file_type", "xml")))
        conn.commit()  
        logging.info(f"Inserted into SQLite: {metadata.get('document_id')}")  
    except Exception as e:
        logging.error(f"SQLite insertion failed: {str(e)}")  
    finally:
        conn.close()  

def process_xml_files(raw_dir="data/raw"):
    """Process all XML files in raw directory"""
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
    for root_dir, _, files in os.walk(raw_dir):
        for file in files:
            if file.lower().endswith('.xml'):
                xml_path = os.path.join(root_dir, file)
                if metadata := parse_xml_metadata(xml_path):
                    metadata["file_type"] = "xml"
                    metadata_list.append(metadata)
                    if save_path := save_metadata(metadata):
                        insert_into_sqlite(metadata)
    
    return metadata_list

if __name__ == "__main__":
    process_xml_files()