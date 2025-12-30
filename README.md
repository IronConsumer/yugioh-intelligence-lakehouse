# 🃏 Yu-Gi-Oh Intelligence Lakehouse

Projet d'ingénierie de données visant à identifier des opportunités d'arbitrage financier sur le marché des cartes Yu-Gi-Oh en comparant les prix entre l'Europe (Cardmarket) et les USA (TCGPlayer).

## 🏗️ Architecture Technique
Le projet suit l'architecture **Medallion** sur un **Data Lakehouse** local utilisant Apache Spark et Delta Lake.

* **Bronze** : Données brutes de l'API YGOPRODeck (JSON immuable).
* **Silver** : Nettoyage, typage (Decimal) et déduplication des cartes.
* **Gold** : Vue métier calculant les écarts de prix (>20%) et les opportunités d'achat.



## 🛠️ Stack Technique
* **Moteur de calcul** : Apache Spark 3.5.0
* **Format de stockage** : Delta Lake (Acid transactions)
* **Langage** : Python 3.12
* **Gestionnaire de dépendances** : [uv](https://github.com/astral-sh/uv)
* **Exploration** : DuckDB & Jupyter Notebooks

## 🚀 Installation & Utilisation
1. Cloner le repo : `git clone ...`
2. Installer les dépendances : `uv sync`
3. Lancer le pipeline complet :
   ```bash
   uv run main.py