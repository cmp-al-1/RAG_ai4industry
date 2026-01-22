import json
import csv
from pathlib import Path
import streamlit as st
from langchain_core.documents import Document
from pypdf import PdfReader
from src.services.pixtral import PixtralPDFProcessor
import os

def load_txt(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    return [Document(page_content=content, metadata={"source": file_path, "type": "txt"})]

def load_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    content = json.dumps(data, indent=2, ensure_ascii=False)
    return [Document(page_content=content, metadata={"source": file_path, "type": "json"})]

def load_csv(file_path):
    documents = []
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            content = "\n".join([f"{k}: {v}" for k, v in row.items()])
            documents.append(
                Document(page_content=content, metadata={"source": file_path, "type": "csv", "row": i})
            )
    return documents

def load_pdf(file_path):
    """Version classique pypdf (fallback)"""
    reader = PdfReader(file_path)
    documents = []
    for i, page in enumerate(reader.pages):
        text = page.extract_text()
        if text.strip():
            documents.append(
                Document(page_content=text, metadata={"source": file_path, "type": "pdf", "page": i})
            )
    return documents

def load_pdf_with_pixtral(file_path, mistral_api_key):
    """
    Charge un PDF avec traitement Pixtral optionnel.
    Fallback gracieux vers pypdf en cas d'erreur.
    """
    # Récupérer le paramètre use_pixtral depuis session_state
    use_pixtral = st.session_state.get('use_pixtral', True)

    if not use_pixtral:
        return load_pdf(file_path)

    try:
        processor = PixtralPDFProcessor(
            mistral_api_key=mistral_api_key,
            model="pixtral-12b-2409",
            cache_images=False
        )

        # Callback de progression pour Streamlit
        def progress_callback(current, total):
            # st.sidebar.info(f"🔍 Analyse Pixtral: Page {current}/{total}")
            print(f"🔍 Analyse Pixtral: Page {current}/{total}")

        documents = processor.process_pdf_complete(
            file_path,
            dpi=200,
            progress_callback=progress_callback
        )

        if documents:
            # st.sidebar.success(f"✅ {len(documents)} chunks enrichis créés avec Pixtral!")
            print(f"✅ {len(documents)} chunks enrichis créés avec Pixtral!")

        return documents

    except Exception as e:
        # st.warning(f"⚠️ Erreur Pixtral pour {Path(file_path).name}, fallback sur pypdf: {e}")
        print(f"⚠️ Erreur Pixtral pour {Path(file_path).name}, fallback sur pypdf: {e}")
        return load_pdf(file_path)

def load_image(file_path, mistral_api_key):
    """
    Charge une image avec traitement Pixtral.
    """
    try:
        processor = PixtralPDFProcessor(
            mistral_api_key=mistral_api_key,
            model="pixtral-12b-2409",
            cache_images=False
        )

        # st.toast(f"🔍 Analyse Pixtral de l'image...", icon="🔍")
        print(f"🔍 Analyse Pixtral de l'image: {file_path}")
        
        documents = processor.process_image_complete(file_path)

        if documents:
            # st.toast(f"✅ Image analysée avec succès!", icon="✅")
            print(f"✅ Image {Path(file_path).name} analysée avec succès! ({len(documents)} chunks)")

        return documents

    except Exception as e:
        # st.toast(f"⚠️ Erreur Pixtral pour l'image {Path(file_path).name}: {e}", icon="⚠️")
        print(f"⚠️ Erreur Pixtral pour l'image {Path(file_path).name}: {e}")
        return []

def load_documents_from_directory(directory, mistral_api_key):
    all_documents = []
    data_path = Path(directory)

    if not data_path.exists():
        return all_documents

    # Map extensions to loader functions; lambda to pass extra args if needed
    loaders = {
        '.txt': load_txt,
        '.json': load_json,
        '.csv': load_csv,
        '.pdf': load_pdf,
        '.png': lambda f: load_image(f, mistral_api_key),
        '.jpg': lambda f: load_image(f, mistral_api_key),
        '.jpeg': lambda f: load_image(f, mistral_api_key)
    }

    for file_path in data_path.rglob('*'):
        if file_path.is_file():
            ext = file_path.suffix.lower()
            if ext in loaders:
                try:
                    if ext in ['.pdf', '.png', '.jpg', '.jpeg']:
                        docs = loaders[ext](str(file_path))
                    else:
                        docs = loaders[ext](str(file_path))
                    all_documents.extend(docs)
                except Exception as e:
                    print(f"Erreur lors du chargement de {file_path.name}: {e}")

    return all_documents
