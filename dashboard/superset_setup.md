# Superset Dashboard Setup Guide

## 1. Access Superset
URL: http://localhost:8088
Login: admin / admin123

---

## 2. Connect Superset to Trino

Settings → Database Connections → + Database

**Select:** Trino

**SQLAlchemy URI:**
```
trino://admin@trino:8080/iceberg
```

**Display Name:** Lakehouse Trino

**Test Connection** → should show "Connection looks good!"

**Save.**

---

## 3. Add Datasets (one per Gold table)

Data → Datasets → + Dataset

Select:
- Database: Lakehouse Trino
- Schema: gold
- Table: (select each one below)

Add these datasets:
| Dataset | Table |
|---|---|
| CA Journalier | gold_ca_daily |
| CA Mensuel | gold_ca_monthly |
| CA par Canal | gold_ca_by_canal |
| Top Produits | gold_top_products |
| Ventes par Région | gold_sales_by_region |
| Panier Client | gold_customer_basket |
| Taux de Retour | gold_return_rate |
| Ventes par Catégorie | gold_sales_by_category |
| Segments Clients | gold_sales_by_segment |
| Tendance Journalière | gold_daily_trend |
| Alertes Stock | gold_stock_alert |
| RFM Clients | gold_client_rfm |
| Performance Canal | gold_canal_performance |

---

## 4. Create Dashboard Charts

### Chart 1 — CA Mensuel (Line Chart)
- Dataset: CA Mensuel
- Chart type: Line Chart
- X-axis: mois_label
- Metrics: SUM(ca_total)
- Title: "Évolution du CA Mensuel"

### Chart 2 — CA par Canal (Pie Chart)
- Dataset: CA par Canal
- Chart type: Pie Chart
- Dimension: nom_canal
- Metric: SUM(ca_total)
- Title: "Répartition CA par Canal"

### Chart 3 — Top 10 Produits (Bar Chart)
- Dataset: Top Produits
- Chart type: Bar Chart (horizontal)
- Y-axis: nom
- Metric: SUM(total_quantite_vendue)
- Sort: descending
- Title: "Top 10 Produits les Plus Vendus"

### Chart 4 — Ventes par Région (Map or Bar)
- Dataset: Ventes par Région
- Chart type: Bar Chart
- X-axis: region
- Metric: SUM(ca_total)
- Title: "CA par Région"

### Chart 5 — Taux de Retour (Bar Chart)
- Dataset: Taux de Retour
- Chart type: Bar Chart
- X-axis: nom
- Metric: MAX(taux_retour_pct)
- Filters: nb_retours > 0
- Title: "Taux de Retour par Produit (Top 10)"

### Chart 6 — Segments RFM (Pie Chart)
- Dataset: RFM Clients
- Chart type: Pie Chart
- Dimension: rfm_segment
- Metric: COUNT(client_id)
- Title: "Segmentation RFM Clients"

### Chart 7 — Tendance + Moyenne Mobile (Line Chart)
- Dataset: Tendance Journalière
- Chart type: Line Chart
- X-axis: date_vente
- Metrics: ca_jour, moyenne_mobile_7j, moyenne_mobile_30j
- Title: "Tendance CA avec Moyennes Mobiles"

### Chart 8 — Alertes Stock (Table)
- Dataset: Alertes Stock
- Chart type: Table
- Columns: nom, categorie, stock_total, jours_de_stock, alerte
- Filters: alerte IN ('🔴 RUPTURE', '🟠 CRITIQUE')
- Title: "🚨 Produits en Alerte Stock"

### Chart 9 — Segments Clientèle (Bar Chart)
- Dataset: Segments Clients
- Chart type: Bar Chart
- X-axis: segment
- Metrics: ca_total, nb_clients_actifs
- Title: "Performance par Segment Clientèle"

### Chart 10 — CA Hebdomadaire (Area Chart)
- Dataset: CA Mensuel
- Chart type: Area Chart
- X-axis: mois_label
- Metric: SUM(ca_total)
- Title: "CA Cumulé"

---

## 5. Create the Dashboard

Dashboards → + Dashboard → "Lakehouse Omnicanal Analytics"

Drag and drop your charts. Suggested layout:

```
┌─────────────────┬─────────────────┬─────────────────┐
│   CA Global     │  Nb Commandes   │  Panier Moyen   │  ← Big Numbers
├─────────────────┴─────────────────┴─────────────────┤
│         Évolution CA Mensuel (Line)                  │  ← Full width
├─────────────────────────┬───────────────────────────┤
│  CA par Canal (Pie)     │  Top 10 Produits (Bar)    │
├─────────────────────────┼───────────────────────────┤
│  Ventes par Région      │  Segments RFM (Pie)       │
├─────────────────────────┼───────────────────────────┤
│  Taux de Retour (Bar)   │  Alertes Stock (Table)    │
└─────────────────────────┴───────────────────────────┘
```

---

## 6. Add Big Number KPI tiles (top of dashboard)

Chart type: Big Number with Trendline

**CA Global:**
- Dataset: CA Mensuel
- Metric: SUM(ca_total)

**Total Commandes:**
- Dataset: CA Journalier
- Metric: SUM(nb_ventes)

**Panier Moyen:**
- Dataset: CA Journalier
- Metric: AVG(panier_moyen)