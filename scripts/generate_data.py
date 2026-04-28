# scripts/generate_data.py
from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta

fake = Faker('fr_FR')
random.seed(42)

# --- CLIENTS ---
clients = [{
    "client_id": i,
    "nom": fake.last_name(),
    "prenom": fake.first_name(),
    "ville": fake.city(),
    "segment": random.choice(["particulier", "professionnel", "premium"]),
    "date_inscription": fake.date_between(start_date="-3y", end_date="today")
} for i in range(1, 1001)]

# --- PRODUITS ---
categories = ["Electronique", "Vêtements", "Alimentation", "Sport", "Maison"]
produits = [{
    "produit_id": i,
    "nom": fake.word().capitalize() + " " + fake.word(),
    "categorie": random.choice(categories),
    "prix": round(random.uniform(5, 500), 2),
    "marque": fake.company()
} for i in range(1, 201)]

# --- CANAUX ---
canaux = [
    {"canal_id": 1, "nom": "magasin"},
    {"canal_id": 2, "nom": "web"},
    {"canal_id": 3, "nom": "mobile"},
    {"canal_id": 4, "nom": "marketplace"}
]

# --- VENTES ---
ventes = [{
    "vente_id": i,
    "date_vente": fake.date_between(start_date="-2y", end_date="today"),
    "client_id": random.randint(1, 1000),
    "produit_id": random.randint(1, 200),
    "canal_id": random.randint(1, 4),
    "quantite": random.randint(1, 10),
    "montant": round(random.uniform(10, 1000), 2)
} for i in range(1, 50001)]  # 50 000 ventes

# --- STOCKS ---
stocks = [{
    "produit_id": i,
    "depot": random.choice(["Casablanca", "Rabat", "Marrakech", "Tanger"]),
    "quantite_disponible": random.randint(0, 500)
} for i in range(1, 201)]

# --- RETOURS ---
retours = [{
    "retour_id": i,
    "vente_id": random.randint(1, 50000),
    "produit_id": random.randint(1, 200),
    "motif": random.choice(["defectueux", "mauvaise_taille", "ne_correspond_pas", "autre"]),
    "date_retour": fake.date_between(start_date="-1y", end_date="today")
} for i in range(1, 2001)]  # ~4% de retours

# Sauvegarde CSV
pd.DataFrame(clients).to_csv("data/clients.csv", index=False)
pd.DataFrame(produits).to_csv("data/produits.csv", index=False)
pd.DataFrame(canaux).to_csv("data/canaux.csv", index=False)
pd.DataFrame(ventes).to_csv("data/ventes.csv", index=False)
pd.DataFrame(stocks).to_csv("data/stocks.csv", index=False)
pd.DataFrame(retours).to_csv("data/retours.csv", index=False)

print("Données générées avec succès !")