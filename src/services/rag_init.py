import streamlit as st
from qdrant_client import QdrantClient
from langchain_mistralai import MistralAIEmbeddings, ChatMistralAI
from langchain_qdrant import QdrantVectorStore
from langchain_text_splitters import RecursiveCharacterTextSplitter
from src.core.hybrid_rag import HybridRAG
from src.services.loaders import load_documents_from_directory

@st.cache_resource
def init_components(mistral_api_key, qdrant_endpoint, qdrant_api_key):
    qdrant_client = QdrantClient(
        url=qdrant_endpoint,
        api_key=qdrant_api_key
    )

    embeddings = MistralAIEmbeddings(
        model="mistral-embed",
        mistral_api_key=mistral_api_key
    )

    llm = ChatMistralAI(
        model="mistral-small-latest",
        mistral_api_key=mistral_api_key,
        temperature=0
    )

    hybrid_rag = HybridRAG()

    return qdrant_client, embeddings, llm, hybrid_rag

@st.cache_resource
def load_and_index_documents(_qdrant_client, _embeddings, collection_name, qdrant_endpoint, qdrant_api_key, mistral_api_key):
    documents = load_documents_from_directory("data", mistral_api_key)

    if not documents:
        return None, 0

    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000,
        chunk_overlap=200
    )
    splits = text_splitter.split_documents(documents)

    # Vérifier si la collection existe déjà
    try:
        collections = _qdrant_client.get_collections().collections
        collection_exists = any(c.name == collection_name for c in collections)

        if collection_exists:
            # Collection existe, l'utiliser directement
            vector_store = QdrantVectorStore(
                client=_qdrant_client,
                collection_name=collection_name,
                embedding=_embeddings
            )
            return vector_store, len(splits)
    except:
        pass

    # Collection n'existe pas, la créer
    vector_store = QdrantVectorStore.from_documents(
        splits,
        _embeddings,
        url=qdrant_endpoint,
        api_key=qdrant_api_key,
        collection_name=collection_name
    )

    return vector_store, len(splits)
