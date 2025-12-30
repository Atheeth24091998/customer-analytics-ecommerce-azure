# E-Commerce Intelligence Platform (Business-Driven Analytics Project)

## Overview
This project is a business-focused data analytics and data science platform built on top of
a real-world e-commerce dataset.

The goal is to simulate how an analytics team in an e-commerce company would:
- Understand business performance
- Analyze customer behavior
- Identify growth and retention opportunities
- Support decision-making with data

At the current stage (Day 1–2), the focus is on **business understanding, data exploration,
and KPI baseline creation**. Will learn the concepts and build in the next 10 days during my university holidays.

---

## Business Problem
E-commerce companies generate large amounts of data but often struggle to turn it into
clear business insights.

Key challenges addressed in this project:
- Identifying high-value customer segments
- Understanding why customers churn
- Measuring the impact of pricing and discount strategies
- Translating raw data into business-ready metrics

---

## Business Objectives
- Track core e-commerce performance KPIs
- Understand customer purchase behavior
- Identify revenue-driving product and customer segments
- Build a foundation for predictive and experimental analysis

---

## Core Business KPIs
The following KPIs are defined and will be tracked throughout the project:

- Revenue & Revenue Growth Rate
- Average Order Value (AOV)
- Customer Lifetime Value (CLV)
- Churn Rate
- Repeat Purchase Rate
- Net Promoter Score (NPS – proxy using review data)

---

## Key Business Questions
1. Which customer segments generate the most revenue?
2. What factors indicate that a customer is likely to churn?
3. Which products are frequently bought together? (Market Basket Analysis)
4. Would offering discounts to at-risk customers improve retention?
5. How do delivery experience and reviews impact repeat purchases?

---

## Planned Analytics & Data Science Use Cases
The project is designed to evolve step by step and will include:

- Exploratory Data Analysis (EDA)
- KPI monitoring and trend analysis
- Customer segmentation
- Market Basket Analysis
- Churn prediction modeling
- Retention experiment simulation (A/B-style analysis)
- Business insight reporting and visualization

---

## Data Architecture
The project follows a simple analytics-engineering style architecture:

- **Bronze Layer:** Raw ingested data
- **Silver Layer:** Cleaned and transformed data
- **Gold Layer:** Business-ready analytics tables

Optimized data formats:
- **Parquet** for performance and compression

---

## Technology Stack
- **Languages & Libraries:** Python, Pandas, NumPy, Scikit-Learn, Matplotlib, Seaborn
- **Cloud Storage:** Azure Blob Storage
- **ML & Analytics:** K-Means, Random Forest, RFM Analysis, Apriori Algorithm, Prophet/ARIMA
- **BI & Visualization:** Power BI
- **Data Formats:** CSV (raw), Parquet (processed)
- **Environment & Deployment:** Docker (planned)
- **Engineering:**FastAPI, Docker, pytest, logging, Makefile

---

## Current Project Status
[X] Project structure and environment setup  
[X] Business objectives and KPIs defined  
[X] Created a Entitiy Relationship diagram for the tables
[X] Exploratory Data Analysis and baseline KPI calculation 


---

## Expected Deliverables
- Reproducible data pipeline
- Business KPI tables
- Interactive Power BI dashboard
- Churn prediction model
- Business experiment analysis
- Well-documented, production-ready repository

---

## Learning Goals
- Business-first analytics thinking
- KPI-driven decision making
- Data engineering fundamentals
- Applied data science for business problems
- Clear communication of insights

---

## Dataset
This project uses the public **Olist E-commerce Dataset** for educational purposes.
