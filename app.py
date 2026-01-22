import os
import streamlit as st
from pathlib import Path
from dotenv import load_dotenv

from src.core.hybrid_rag import QueryExamples
from src.ui.dashboard import render_dashboard
from src.services.rag_init import init_components, load_and_index_documents
from src.ui.styles import apply_custom_styles

# Configuration
load_dotenv()

MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY")
QDRANT_ENDPOINT = os.getenv("QDRANT_ENDPOINT")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
COLLECTION_NAME = "documents_rag"

def main():
    st.set_page_config(
        page_title="GROUPE 1 - RAG UI",
        layout="wide",
        page_icon="🤖",
        initial_sidebar_state="expanded"
    )

    # Appliquer le styles
    apply_custom_styles()

    # En-tête
    st.title("GROUPE 1 - RAG UI")
    st.markdown("""
        <p style='font-size: 1.2rem; color: #64748b; margin-bottom: 2rem;'>
            RAG intelligent - recherche vectorielle et graphique
        </p>
    """, unsafe_allow_html=True)

    # Initialisation
    qdrant_client, embeddings, llm, hybrid_rag = init_components(
        MISTRAL_API_KEY, QDRANT_ENDPOINT, QDRANT_API_KEY
    )


    # Chargement automatique Neo4j (une seule fois par session)
    if "neo4j_initialized" not in st.session_state:
        st.toast("⏳ Start loading Neo4j Graph...", icon="⏳")
        with st.spinner("Initialisation de la base de données Neo4j..."):
            from src.services.neo4j_loader import Neo4jLoader
            loader = Neo4jLoader()
            try:
                loader.load_all()
                st.session_state["neo4j_initialized"] = True
                st.toast("✅ Neo4j Graph ready!", icon="✅")
            except Exception as e:
                st.error(f"Erreur chargement Neo4j: {e}")
            finally:
                loader.close()

    # Sidebar
    with st.sidebar:
        st.markdown("### 📤 Importer des documents")
        st.markdown("""
            <div style='background: rgba(59, 130, 246, 0.1); padding: 0.5rem; border-radius: 6px; margin-bottom: 0.75rem; font-size: 0.85rem;'>
                💡 Glissez-déposez vos fichiers ci-dessous
            </div>
        """, unsafe_allow_html=True)

        uploaded_files = st.file_uploader(
            "Choisissez des fichiers",
            type=['txt', 'json', 'csv', 'pdf', 'png', 'jpg', 'jpeg'],
            accept_multiple_files=True,
            help="Formats supportés: TXT, JSON, CSV, PDF, PNG, JPG, JPEG",
            label_visibility="collapsed"
        )

        if uploaded_files:
            st.markdown(f"**{len(uploaded_files)} fichier(s) sélectionné(s)**")

            if st.button("📥 Sauvegarder et indexer", use_container_width=True, type="primary"):
                data_dir = Path("data")
                data_dir.mkdir(exist_ok=True)

                success_count = 0
                error_count = 0

                with st.spinner("💾 Sauvegarde en cours..."):
                    for uploaded_file in uploaded_files:
                        try:
                            file_path = data_dir / uploaded_file.name
                            with open(file_path, "wb") as f:
                                f.write(uploaded_file.getbuffer())
                            success_count += 1
                        except Exception as e:
                            st.error(f"❌ Erreur pour {uploaded_file.name}: {e}")
                            error_count += 1

                if success_count > 0:
                    st.success(f"✅ {success_count} fichier(s) sauvegardé(s)!")
                    st.info("🔄 Rechargez la page pour indexer les nouveaux documents")

                    if st.button("🔄 Recharger maintenant", use_container_width=True):
                        st.cache_resource.clear()
                        st.rerun()

                if error_count > 0:
                    st.warning(f"⚠️ {error_count} erreur(s) rencontrée(s)")

        st.divider()

        st.markdown("### 🔧 Admin")
        col1, col2 = st.columns(2)
        with col1:
             if st.button("🗑️ Reset Neo4j", use_container_width=True, help="Vider la base Neo4j"):
                with st.spinner("Nettoyage de Neo4j..."):
                    from src.services.neo4j_loader import Neo4jLoader
                    loader = Neo4jLoader()
                    try:
                        loader.clear_database()
                        st.success("✅ Neo4j vidé !")
                    except Exception as e:
                        st.error(f"Erreur: {e}")
                    finally:
                        loader.close()

        with col2:
            if st.button("🗑️ Reset Qdrant", use_container_width=True, help="Réinitialise la collection Qdrant"):
                try:
                    qdrant_client.delete_collection(COLLECTION_NAME)
                    st.cache_resource.clear()
                    st.success("✅ Qdrant vidé")
                    st.rerun()
                except Exception as e:
                    st.warning(f"⚠️ Erreur: {e}")

        st.divider()

        st.markdown("### 🧠 Modèle de Raisonnement")
        # Liste des modèles disponibles
        available_models = ["mistral-small-latest", "mistral-large-latest", "pixtral-12b-2409"]
        
        # Sélecteur
        selected_model = st.selectbox(
            "Choisir le LLM:",
            available_models,
            index=0,
            help="Le modèle utilisé pour générer les réponses finales"
        )

        # Mise à jour dynamique du modèle si changement
        if "current_model" not in st.session_state:
            st.session_state["current_model"] = selected_model
        
        if st.session_state["current_model"] != selected_model:
            hybrid_rag.set_model(selected_model)
            st.session_state["current_model"] = selected_model
            st.toast(f"🤖 Modèle changé pour {selected_model}")

        st.divider()

        st.markdown("### 👥 Crédits")
        st.markdown("""
            <div style='font-size: 0.9rem; color: #64748b;'>
                <strong>Développé par le groupe 1:</strong><br>
                • Enzo<br>
                • Kyllian<br>
                • Romain<br>
                • Will<br>
                • Yovèn
            </div>
        """, unsafe_allow_html=True)

    # Chargement et indexation
    with st.spinner("⏳ Chargement et indexation des documents..."):
        vector_store, num_chunks = load_and_index_documents(
            qdrant_client, embeddings, COLLECTION_NAME, QDRANT_ENDPOINT, QDRANT_API_KEY, MISTRAL_API_KEY
        )

    if vector_store is None:
        st.markdown("""
            <div style='background: rgba(251, 191, 36, 0.2); padding: 1rem; border-radius: 8px; border-left: 4px solid #fbbf24; margin: 1rem 0;'>
                ⚠️ <strong>Aucun document trouvé</strong><br>
                Veuillez ajouter des documents dans le dossier <code>data/</code>
            </div>
        """, unsafe_allow_html=True)
        st.info("📝 Formats supportés: .txt, .json, .csv, .pdf")
        return

    # Tabs
    tab_router, tab_dashboard = st.tabs([
        "🧭 ChatBOT",
        "📊 Dashboard Métriques"
    ])

    # TAB 1: Routeur intelligent
    with tab_router:
        st.markdown("### 🧭 CHAT BOT")

        with st.form("chat_form"):
            question_auto = st.text_input(
                "💬 Posez votre question:",
                placeholder="Posez n'importe quelle question...",
                key="auto",
                help="Le système analysera votre question et choisira automatiquement la meilleure stratégie"
            )
            submitted = st.form_submit_button("Envoyer", type="primary")
        
        if submitted and question_auto:
            # Analyse rapide (sans explication détaillée visuelle)
            routing = hybrid_rag.explain_routing(question_auto)
            strategy_display = "🧠 Mode Multi-Hop (Graph + Vector)" if routing['strategy'] == "multi_hop" else "🔎 Mode Simple (Vector)"
            
            st.markdown(f"""
                <div style='margin-bottom: 1rem; color: #64748b; font-size: 0.9rem;'>
                    Méthode utilisée: <strong>{strategy_display}</strong>
                </div>
            """, unsafe_allow_html=True)

            with st.spinner("🤖 Traitement intelligent de la question..."):
                result = hybrid_rag.query(question_auto, vector_store)

            st.markdown("---")
            st.markdown("### ✨ Réponse")
            st.markdown(f"""
                <div style='background: rgba(239, 68, 68, 0.05); padding: 1.5rem; border-radius: 8px; margin: 1rem 0; font-size: 1.05rem; line-height: 1.6;'>
                    {result["answer"]}
                </div>
            """, unsafe_allow_html=True)

            st.markdown("### 📊 Sources consultées")
            
            source_tab_vector, source_tab_graph = st.tabs(["📚 Documents (Qdrant)", "🕸️ Relations (Neo4j)"])
            
            # --- TAB VECTOR ---
            with source_tab_vector:
                if result["sources"]["vector_docs"]:
                    st.caption(f"**{len(result['sources']['vector_docs'])}** documents pertinents trouvés")
                    
                    # Layout en grille (3 colonnes)
                    cols = st.columns(3)
                    
                    for i, doc in enumerate(result["sources"]["vector_docs"]):
                        source_name = doc.metadata.get('source', 'Inconnu')
                        display_name = Path(source_name).name
                        doc_type = doc.metadata.get('type', 'unknown')
                        
                        # Icône selon le type
                        icon = "📄"
                        if doc_type == 'pdf': icon = "📕"
                        elif doc_type == 'json': icon = "📋"
                        elif doc_type == 'csv': icon = "📊"
                        elif doc_type == 'image': icon = "🖼️"
                        elif doc_type == 'visual': icon = "👁️"
                        
                        # Distribution dans les colonnes
                        with cols[i % 3]:
                            with st.container(border=True):
                                st.markdown(f"**{icon} Source {i+1}**")
                                st.caption(f"_{display_name}_")
                                
                                # Aperçu court
                                preview = doc.page_content[:150].replace("\n", " ") + "..."
                                st.markdown(f"<div style='font-size: 0.85em; color: #cbd5e1; margin-bottom: 10px;'>{preview}</div>", unsafe_allow_html=True)
                                
                                # Détails complets dans un expander
                                with st.expander("🔎 Voir détails"):
                                    st.markdown("**Contenu complet:**")
                                    st.code(doc.page_content, language="text")
                                    st.markdown("**Métadonnées:**")
                                    st.json(doc.metadata)

                else:
                    st.info("Aucun document vectoriel utilisé.")

            # --- TAB GRAPH ---
            with source_tab_graph:
                graph_ctx = result["sources"].get("graph_context", [])
                if graph_ctx and result["strategy"] in ["multi_hop", "hybrid"]:
                    st.caption(f"**{len(graph_ctx)}** étapes de raisonnement graphique")
                    
                    # Layout en grille (3 colonnes)
                    cols = st.columns(3)

                    for idx, item in enumerate(graph_ctx):
                        query_type = item['query_type']
                        results = item.get("results", [])
                        
                        # Distribution dans les colonnes
                        with cols[idx % 3]:
                            with st.container(border=True):
                                st.markdown(f"**🕸️ Étape {idx + 1}**")
                                st.caption(f"_{query_type}_")
                                
                                # Aperçu des résultats
                                if results:
                                    num_res = len(results)
                                    first_res = str(results[0])[:100] + "..."
                                    st.markdown(f"<div style='font-size: 0.85em; color: #cbd5e1; margin-bottom: 5px;'>{num_res} résultat(s)</div>", unsafe_allow_html=True)
                                    st.markdown(f"<div style='font-size: 0.8em; color: #94a3b8; margin-bottom: 10px; font-style: italic;'>Ex: {first_res}</div>", unsafe_allow_html=True)
                                else:
                                    st.info("Aucun résultat direct")

                                # Détails complets dans un expander
                                with st.expander("🔎 Voir les données"):
                                    st.markdown(f"**Requête:** `{query_type}`")
                                    if results:
                                        for res in results:
                                            st.code(str(res), language="json")
                                    else:
                                        st.warning("Aucune donnée retournée par le graphe.")
                else:
                    st.info("Le graphe Neo4j n'a pas été sollicité pour cette réponse (Mode Simple).")

    # TAB 2: Dashboard Métriques
    with tab_dashboard:
        render_dashboard(qdrant_client, hybrid_rag.neo4j_querier, vector_store)

if __name__ == "__main__":
    main()
