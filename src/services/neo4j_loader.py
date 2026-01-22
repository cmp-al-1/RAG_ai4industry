import os
import json
import glob
from dotenv import load_dotenv
from neo4j import GraphDatabase
from src.services.pixtral import PixtralPDFProcessor

load_dotenv()

class Neo4jLoader:
    def __init__(self):
        self.driver = GraphDatabase.driver(
            os.getenv("NEO4J_URI"),
            auth=(os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))
        )

    def close(self):
        self.driver.close()

    def clear_database(self):
        """Supprime toutes les données du graphe"""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            print("Base de données Neo4j nettoyée")

    def create_indexes(self):
        """Crée les index pour optimiser les requêtes"""
        with self.driver.session() as session:
            # Index sur les IDs
            session.run("CREATE INDEX product_id IF NOT EXISTS FOR (p:Product) ON (p.product_id)")
            session.run("CREATE INDEX event_id IF NOT EXISTS FOR (e:Event) ON (e.event_id)")
            session.run("CREATE INDEX trade_show_id IF NOT EXISTS FOR (t:TradeShow) ON (t.event_id)")
            session.run("CREATE INDEX rd_project_id IF NOT EXISTS FOR (r:RDProject) ON (r.project_id)")
            session.run("CREATE INDEX sale_customer IF NOT EXISTS FOR (s:Sale) ON (s.customer_type)")
            print("Index créés avec succès")

    def _find_file(self, default_path, pattern):
        """Cherche un fichier correspondant au pattern si le défaut n'existe pas"""
        if os.path.exists(default_path):
            return default_path
        
        # Chercher dans le dossier data
        candidates = glob.glob(f"data/{pattern}")
        if candidates:
            print(f"ℹ️  Fichier par défaut non trouvé, utilisation de : {candidates[0]}")
            return candidates[0]
        return None

    def load_products(self, products_file="data/greenpower_products.json"):
        """Charge les produits dans Neo4j"""
        actual_file = self._find_file(products_file, "*product*.json")
        
        if not actual_file:
            print(f"⚠️  Aucun fichier de produits trouvé (attendu: {products_file} ou *product*.json)")
            return False

        try:
            with open(actual_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            print(f"❌ Erreur lecture {actual_file}: {e}")
            return False

        with self.driver.session() as session:
            for product in data.get("products", []):
                # Créer le nœud Product
                session.run("""
                    MERGE (p:Product {product_id: $product_id})
                    SET p.name = $name,
                        p.category = $category,
                        p.continuous_power = $continuous_power,
                        p.peak_power = $peak_power,
                        p.battery_capacity = $battery_capacity,
                        p.battery_type = $battery_type,
                        p.solar_capacity = $solar_capacity,
                        p.total_cost = $total_cost,
                        p.avg_selling_price = $avg_selling_price,
                        p.margin_percentage = $margin_percentage,
                        p.co2_reduction = $co2_reduction,
                        p.rental_available = $rental_available
                """,
                    product_id=product["product_id"],
                    name=product["name"],
                    category=product["category"],
                    continuous_power=product["power_output"]["continuous"],
                    peak_power=product["power_output"]["peak"],
                    battery_capacity=product["specifications"]["battery_capacity"],
                    battery_type=product["specifications"]["battery_type"],
                    solar_capacity=product["specifications"]["solar_panel_capacity"],
                    total_cost=product["private_cost_breakdown"]["private_total_cost"],
                    avg_selling_price=product["pricing"]["average_selling_price"],
                    margin_percentage=product["pricing"]["margin_percentage"],
                    co2_reduction=product["co2_reduction"],
                    rental_available=product["rental_available"]
                )

                # Créer le nœud BatteryType et la relation
                session.run("""
                    MERGE (b:BatteryType {type: $battery_type})
                    WITH b
                    MATCH (p:Product {product_id: $product_id})
                    MERGE (p)-[:USES_BATTERY]->(b)
                """,
                    battery_type=product["specifications"]["battery_type"],
                    product_id=product["product_id"]
                )

        print(f"Chargé {len(data.get('products', []))} produits depuis {actual_file}")
        return True

    def parse_revenue(self, revenue_str):
        """Parse revenue string like '€911,750' to float"""
        if isinstance(revenue_str, (int, float)):
            return float(revenue_str)
        # Remove €, spaces, and convert
        cleaned = revenue_str.replace('€', '').replace(',', '').replace(' ', '').strip()
        try:
            return float(cleaned)
        except:
            return 0.0

    def load_events(self, events_file="data/greenpower_events.json"):
        """Charge les événements (trade shows, powered events) dans Neo4j"""
        actual_file = self._find_file(events_file, "*event*.json")
        
        if not actual_file:
            print(f"⚠️  Aucun fichier d'événements trouvé (attendu: {events_file} ou *event*.json)")
            return False

        try:
            with open(actual_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            print(f"❌ Erreur lecture {actual_file}: {e}")
            return False

        with self.driver.session() as session:
            # Charger les salons (trade shows)
            for event in data.get("trade_shows_exhibitions", []):
                # Créer le nœud TradeShow
                session.run("""
                    MERGE (t:TradeShow {event_id: $event_id})
                    SET t.name = $name,
                        t.type = $type,
                        t.location = $location,
                        t.date = $date,
                        t.leads_generated = $leads_generated,
                        t.total_sales = $total_sales
                """,
                    event_id=event["event_id"],
                    name=event["event_name"],
                    type=event["type"],
                    location=event["location"],
                    date=event["date"],
                    leads_generated=event["sales_data"]["leads_generated"],
                    total_sales=self.parse_revenue(event["sales_data"]["total_sales"])
                )

                # Créer les produits affichés et les relations
                for product_id in event["greenpower_participation"].get("models_displayed", []):
                    session.run("""
                        MATCH (t:TradeShow {event_id: $event_id})
                        MATCH (p:Product {product_id: $product_id})
                        MERGE (p)-[:DISPLAYED_AT]->(t)
                    """,
                        event_id=event["event_id"],
                        product_id=product_id
                    )

                # Créer les ventes par type de client
                for customer_type in ["particuliers", "entreprises", "collectivites"]:
                    sales = event["sales_data"]["sales_closed"].get(customer_type, {})
                    if sales.get("units", 0) > 0:
                        # Créer nœud Sale
                        sale_id = f"{event['event_id']}_{customer_type}"
                        session.run("""
                            MERGE (s:Sale {sale_id: $sale_id})
                            SET s.customer_type = $customer_type,
                                s.units = $units,
                                s.total_revenue = $total_revenue
                            WITH s
                            MATCH (t:TradeShow {event_id: $event_id})
                            MERGE (s)-[:SOLD_AT]->(t)
                        """,
                            sale_id=sale_id,
                            customer_type=customer_type,
                            units=sales["units"],
                            total_revenue=self.parse_revenue(sales["total_revenue"]),
                            event_id=event["event_id"]
                        )

                        # Lier les produits vendus
                        for product_str in sales.get("products", []):
                            # Parser "PG-M01 x3" -> ("PG-M01", 3)
                            parts = product_str.split(" x")
                            if len(parts) == 2:
                                product_id = parts[0]
                                quantity = int(parts[1])
                                session.run("""
                                    MATCH (s:Sale {sale_id: $sale_id})
                                    MATCH (p:Product {product_id: $product_id})
                                    MERGE (s)-[r:INCLUDES_PRODUCT]->(p)
                                    SET r.quantity = $quantity
                                """,
                                    sale_id=sale_id,
                                    product_id=product_id,
                                    quantity=quantity
                                )

            print(f"Chargé {len(data.get('trade_shows_exhibitions', []))} salons")

            # Charger les événements alimentés (powered events)
            for event in data.get("powered_events", []):
                # Créer le nœud Event
                session.run("""
                    MERGE (e:Event {event_id: $event_id})
                    SET e.name = $name,
                        e.type = $type,
                        e.location = $location,
                        e.date = $date,
                        e.attendees = $attendees,
                        e.runtime = $runtime,
                        e.fuel_saved = $fuel_saved,
                        e.co2_reduction = $co2_reduction
                """,
                    event_id=event["event_id"],
                    name=event["event_name"],
                    type=event["type"],
                    location=event["location"],
                    date=event["date"],
                    attendees=event["power_deployment"].get("attendees", "N/A"),
                    runtime=event["power_deployment"]["runtime"],
                    fuel_saved=event["power_deployment"]["fuel_saved"],
                    co2_reduction=event["power_deployment"]["co2_reduction"]
                )

                # Créer les relations avec les produits déployés
                for model_str in event["power_deployment"]["models_used"]:
                    # Parser "PG-U01 x2" -> ("PG-U01", 2)
                    parts = model_str.split(" x")
                    if len(parts) == 2:
                        product_id = parts[0]
                        quantity = int(parts[1])
                        session.run("""
                            MATCH (e:Event {event_id: $event_id})
                            MATCH (p:Product {product_id: $product_id})
                            MERGE (p)-[r:DEPLOYED_AT]->(e)
                            SET r.quantity = $quantity
                        """,
                            event_id=event["event_id"],
                            product_id=product_id,
                            quantity=quantity
                        )
                    else:
                        # Format sans quantité
                        product_id = model_str
                        session.run("""
                            MATCH (e:Event {event_id: $event_id})
                            MATCH (p:Product {product_id: $product_id})
                            MERGE (p)-[r:DEPLOYED_AT]->(e)
                            SET r.quantity = 1
                        """,
                            event_id=event["event_id"],
                            product_id=product_id
                        )

            print(f"Chargé {len(data.get('powered_events', []))} événements alimentés depuis {actual_file}")
            return True

    def load_rd_projects(self, rd_file="data/greenpower_rd_innovations.json"):
        """Charge les projets R&D dans Neo4j"""
        actual_file = self._find_file(rd_file, "*rd*.json")
        
        if not actual_file:
            print(f"⚠️  Aucun fichier R&D trouvé (attendu: {rd_file} ou *rd*.json)")
            return False

        try:
            with open(actual_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            print(f"❌ Erreur lecture {actual_file}: {e}")
            return False

        with self.driver.session() as session:
            for project in data.get("active_rd_projects", []):
                # Créer le nœud RDProject
                session.run("""
                    MERGE (r:RDProject {project_id: $project_id})
                    SET r.name = $name,
                        r.status = $status,
                        r.objective = $objective,
                        r.projected_savings = $projected_savings
                """,
                    project_id=project["project_id"],
                    name=project["project_name"],
                    status=project["status"],
                    objective=project["objective"],
                    projected_savings=project.get("projected_annual_savings", "N/A")
                )

                # Lier aux produits cibles
                for product_id in project.get("target_products", []):
                    session.run("""
                        MATCH (r:RDProject {project_id: $project_id})
                        MATCH (p:Product {product_id: $product_id})
                        MERGE (r)-[:TARGETS_PRODUCT]->(p)
                    """,
                        project_id=project["project_id"],
                        product_id=product_id
                    )

        print(f"Chargé {len(data.get('active_rd_projects', []))} projets R&D depuis {actual_file}")
        return True

    def load_image(self, image_path="data/exemple.jpg"):
        """Charge une image, l'analyse avec Pixtral et crée un nœud Image dans Neo4j"""
        if not os.path.exists(image_path):
            # Silencieux si l'image n'existe pas, ou warning
            # print(f"⚠️ Image non trouvée: {image_path}")
            return False

        try:
            print(f"🖼️ Analyse de l'image {image_path} avec Pixtral...")
            processor = PixtralPDFProcessor(
                mistral_api_key=os.getenv("MISTRAL_API_KEY"),
                model="pixtral-12b-2409"
            )
            
            # Utiliser process_image_complete qu'on a ajouté récemment
            documents = processor.process_image_complete(image_path)
            
            if not documents:
                print("❌ Aucune analyse produite par Pixtral")
                return False
                
            # Extraire une description globale (concaténation des chunks)
            description = "\n".join([doc.page_content for doc in documents])
            filename = os.path.basename(image_path)

            # Stocker dans Neo4j
            with self.driver.session() as session:
                session.run("""
                    MERGE (i:Image {filename: $filename})
                    SET i.description = $description,
                        i.path = $path,
                        i.analyzed_at = datetime()
                """,
                    filename=filename,
                    description=description,
                    path=image_path
                )
            
            print(f"✅ Image {filename} chargée et analysée dans Neo4j!")
            return True

        except Exception as e:
            print(f"❌ Erreur lors du chargement de l'image: {e}")
            return False

    def load_all(self):
        """Charge toutes les données"""
        print("Début du chargement des données dans Neo4j...")
        self.clear_database()
        self.create_indexes()

        # Charger les données sans se soucier des noms exacts
        files_loaded = 0

        print("\nChargement des fichiers de données...")
        if self.load_products():
            files_loaded += 1

        if self.load_events():
            files_loaded += 1

        if self.load_rd_projects():
            files_loaded += 1

        # Chargement de l'image exemple (demandé spécifiquement)
        if self.load_image("data/exemple.jpg"):
            files_loaded += 1

        if files_loaded == 0:
            print("\n⚠️  Aucun fichier de données JSON pertinent trouvé dans data/")
            print("   Assurez-vous d'avoir des fichiers contenant 'product', 'event' ou 'rd' dans leur nom.")
        else:
            print(f"\n✅ {files_loaded} fichier(s) chargé(s) avec succès!")
            print("   Chargement terminé!")

    def verify_data(self):
        """Vérifie les données chargées"""
        with self.driver.session() as session:
            # Compter les nœuds
            result = session.run("MATCH (n) RETURN labels(n) as label, count(n) as count")
            print("\nStatistiques du graphe:")
            for record in result:
                print(f"  {record['label'][0]}: {record['count']} nœuds")

            # Compter les relations
            result = session.run("MATCH ()-[r]->() RETURN type(r) as rel_type, count(r) as count")
            print("\nRelations:")
            for record in result:
                print(f"  {record['rel_type']}: {record['count']}")

if __name__ == "__main__":
    loader = Neo4jLoader()
    try:
        loader.load_all()
        loader.verify_data()
    finally:
        loader.close()
