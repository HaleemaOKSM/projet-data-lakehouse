-- =============================================================
--  TRINO ANALYTICAL QUERIES
--  Catalog: iceberg  |  Schema: gold
--  Run at: http://localhost:8080
-- =============================================================

-- ─────────────────────────────────────────────────────────────
--  HOW TO CONNECT TO TRINO CLI
--  docker exec -it trino trino --catalog iceberg --schema gold
-- ─────────────────────────────────────────────────────────────

-- Verify all gold tables are visible
SHOW TABLES IN iceberg.gold;


-- ═════════════════════════════════════════════════════════════
--  1. CHIFFRE D'AFFAIRES
-- ═════════════════════════════════════════════════════════════

-- CA par jour (last 30 days)
SELECT
    date_vente,
    ca_total,
    nb_ventes,
    panier_moyen,
    total_quantite
FROM iceberg.gold.gold_ca_daily
ORDER BY date_vente DESC
LIMIT 30;

-- CA par mois
SELECT
    mois_label,
    ca_total,
    nb_ventes,
    panier_moyen,
    total_quantite
FROM iceberg.gold.gold_ca_monthly
ORDER BY annee, mois;

-- CA par semaine
SELECT
    annee,
    semaine,
    ca_total,
    nb_ventes,
    panier_moyen
FROM iceberg.gold.gold_ca_weekly
ORDER BY annee, semaine;

-- CA total global
SELECT
    SUM(ca_total)  AS ca_global,
    SUM(nb_ventes) AS total_ventes,
    ROUND(AVG(panier_moyen), 2) AS panier_moyen_global
FROM iceberg.gold.gold_ca_monthly;


-- ═════════════════════════════════════════════════════════════
--  2. CA PAR CANAL DE VENTE
-- ═════════════════════════════════════════════════════════════

SELECT
    nom_canal,
    ca_total,
    nb_ventes,
    panier_moyen,
    total_quantite,
    part_ca_pct
FROM iceberg.gold.gold_ca_by_canal
ORDER BY ca_total DESC;

-- Canal performance with month-over-month growth
SELECT
    nom_canal,
    mois_label,
    ca_total,
    nb_ventes,
    nb_clients_uniques,
    croissance_pct
FROM iceberg.gold.gold_canal_performance
ORDER BY nom_canal, annee, mois;


-- ═════════════════════════════════════════════════════════════
--  3. TOP PRODUITS
-- ═════════════════════════════════════════════════════════════

-- Top 10 produits les plus vendus
SELECT
    rang,
    nom,
    categorie,
    marque,
    prix,
    total_quantite_vendue,
    ca_total,
    nb_transactions
FROM iceberg.gold.gold_top_products
ORDER BY rang;

-- Top produits par catégorie
SELECT
    categorie_produit,
    mois_label,
    ca_total,
    nb_ventes,
    total_quantite
FROM iceberg.gold.gold_sales_by_category
ORDER BY categorie_produit, mois_label;


-- ═════════════════════════════════════════════════════════════
--  4. VENTES PAR RÉGION / VILLE
-- ═════════════════════════════════════════════════════════════

-- CA par région
SELECT
    region,
    SUM(ca_total)    AS ca_region,
    SUM(nb_ventes)   AS nb_ventes,
    ROUND(AVG(panier_moyen), 2) AS panier_moyen
FROM iceberg.gold.gold_sales_by_region
GROUP BY region
ORDER BY ca_region DESC;

-- CA par ville (top 10)
SELECT
    ville,
    region,
    ca_total,
    nb_ventes,
    panier_moyen
FROM iceberg.gold.gold_sales_by_region
ORDER BY ca_total DESC
LIMIT 10;


-- ═════════════════════════════════════════════════════════════
--  5. CLIENTS
-- ═════════════════════════════════════════════════════════════

-- Panier moyen par client (top 20)
SELECT
    nom,
    prenom,
    ville,
    segment,
    nb_achats,
    ca_total_client,
    panier_moyen,
    total_articles
FROM iceberg.gold.gold_customer_basket
ORDER BY ca_total_client DESC
LIMIT 20;

-- Répartition par segment
SELECT
    segment,
    ca_total,
    nb_ventes,
    nb_clients_actifs,
    panier_moyen,
    ROUND(ca_total * 100.0 / SUM(ca_total) OVER(), 2) AS part_pct
FROM iceberg.gold.gold_sales_by_segment
ORDER BY ca_total DESC;

-- Segmentation RFM complète
SELECT
    rfm_segment,
    COUNT(*)                        AS nb_clients,
    ROUND(AVG(montant_total), 2)    AS panier_moyen,
    ROUND(AVG(recence_jours), 0)    AS recence_moyenne_jours,
    ROUND(AVG(frequence), 1)        AS frequence_moyenne,
    ROUND(SUM(montant_total), 2)    AS ca_segment
FROM iceberg.gold.gold_client_rfm
GROUP BY rfm_segment
ORDER BY ca_segment DESC;

-- Clients champions (VIP list)
SELECT
    nom, prenom, ville, region,
    recence_jours, frequence, montant_total,
    rfm_score, rfm_segment
FROM iceberg.gold.gold_client_rfm
WHERE rfm_segment = '💎 Champion'
ORDER BY montant_total DESC;

-- Clients à risque (win-back campaign)
SELECT
    nom, prenom, ville,
    recence_jours, frequence, montant_total,
    rfm_segment
FROM iceberg.gold.gold_client_rfm
WHERE rfm_segment IN ('😴 A Risque', '💤 Perdu')
ORDER BY montant_total DESC;


-- ═════════════════════════════════════════════════════════════
--  6. RETOURS
-- ═════════════════════════════════════════════════════════════

-- Taux de retour par produit (top 10 problématiques)
SELECT
    nom,
    categorie,
    marque,
    nb_ventes,
    nb_retours,
    taux_retour_pct,
    montant_retours
FROM iceberg.gold.gold_return_rate
WHERE nb_retours > 0
ORDER BY taux_retour_pct DESC
LIMIT 10;

-- Taux de retour par catégorie
SELECT
    categorie,
    SUM(nb_ventes)   AS total_ventes,
    SUM(nb_retours)  AS total_retours,
    ROUND(SUM(nb_retours) * 100.0 / SUM(nb_ventes), 2) AS taux_retour_pct,
    SUM(montant_retours) AS montant_total_retours
FROM iceberg.gold.gold_return_rate
GROUP BY categorie
ORDER BY taux_retour_pct DESC;


-- ═════════════════════════════════════════════════════════════
--  7. STOCK
-- ═════════════════════════════════════════════════════════════

-- Produits en rupture ou critique
SELECT
    nom,
    categorie,
    marque,
    stock_total,
    ventes_par_jour,
    jours_de_stock,
    alerte,
    valeur_stock
FROM iceberg.gold.gold_stock_alert
WHERE alerte IN ('🔴 RUPTURE', '🟠 CRITIQUE')
ORDER BY jours_de_stock;

-- Résumé stock par niveau d'alerte
SELECT
    alerte,
    COUNT(*)              AS nb_produits,
    SUM(stock_total)      AS stock_total,
    SUM(valeur_stock)     AS valeur_totale
FROM iceberg.gold.gold_stock_alert
GROUP BY alerte
ORDER BY alerte;


-- ═════════════════════════════════════════════════════════════
--  8. TENDANCE & DASHBOARD EXÉCUTIF
-- ═════════════════════════════════════════════════════════════

-- Tendance journalière avec moyennes mobiles
SELECT
    date_vente,
    ca_jour,
    nb_ventes,
    nb_clients_actifs,
    moyenne_mobile_7j,
    moyenne_mobile_30j,
    ca_cumule
FROM iceberg.gold.gold_daily_trend
ORDER BY date_vente DESC
LIMIT 90;

-- Affinités produits (cross-sell)
SELECT
    produit_a,
    categorie_a,
    produit_b,
    categorie_b,
    nb_clients_communs
FROM iceberg.gold.gold_product_affinity
ORDER BY nb_clients_communs DESC
LIMIT 20;

-- KPI résumé exécutif (une seule ligne)
SELECT
    ROUND(SUM(ca_total), 2)         AS ca_global,
    SUM(nb_ventes)                  AS total_commandes,
    ROUND(AVG(panier_moyen), 2)     AS panier_moyen_global,
    MAX(ca_total)                   AS meilleur_mois_ca,
    MIN(ca_total)                   AS moins_bon_mois_ca
FROM iceberg.gold.gold_ca_monthly;

