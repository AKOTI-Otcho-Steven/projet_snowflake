import streamlit as st
import plotly.express as px
from snowflake.snowpark.context import get_active_session

# Fonction pour exécuter une requête SQL et retourner un DataFrame pandas
def run_query(session, query):
    df = session.sql(query).to_pandas()
    return df

# Récupérer la session Snowflake active (dans Streamlit Snowflake Projects)
session = get_active_session()

# Requêtes SQL
query_top10_titles = """
SELECT ji."industry_id", jp.title, COUNT(*) AS nb_publications
FROM job_postings jp
JOIN job_industries ji ON jp.job_id = ji."job_id"
GROUP BY ji."industry_id", jp.title
QUALIFY ROW_NUMBER() OVER (PARTITION BY ji."industry_id" ORDER BY COUNT(*) DESC) <= 10
ORDER BY ji."industry_id", nb_publications DESC;
"""

query_top10_salary = """
SELECT ji."industry_id", jp.title, jp.med_salary
FROM job_postings jp
JOIN job_industries ji ON jp.job_id = ji."job_id"
WHERE jp.med_salary IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY ji."industry_id" ORDER BY jp.med_salary DESC) <= 10
ORDER BY ji."industry_id", jp.med_salary DESC;
"""

query_offers_by_company_size = """
SELECT c."company_size", COUNT(*) AS nb_offres
FROM job_postings jp
JOIN companies c ON jp.company_id = c."company_id"
GROUP BY c."company_size"
ORDER BY c."company_size";
"""

query_offers_by_industry = """
SELECT ji."industry_id", COUNT(*) AS nb_offres
FROM job_postings jp
JOIN job_industries ji ON jp.job_id = ji."job_id"
GROUP BY ji."industry_id"
ORDER BY nb_offres DESC;
"""

query_offers_by_work_type = """
SELECT jp.formatted_work_type AS type_emploi, COUNT(*) AS nb_offres
FROM job_postings jp
GROUP BY jp.formatted_work_type
ORDER BY nb_offres DESC;
"""

# Streamlit App
st.title("Analyses des Offres d’Emploi avec Snowflake et Streamlit")

st.header("1. Top 10 des titres de postes les plus publiés par industrie")
df_titles = run_query(session, query_top10_titles)
fig1 = px.bar(df_titles, x="TITLE", y="NB_PUBLICATIONS", color="industry_id",
              title="Top 10 des titres de postes par industrie",
              labels={"TITLE": "Titre de poste", "NB_PUBLICATIONS": "Nombre de publications", "INDUSTRY_ID": "ID Industrie"},
              barmode='group')
st.plotly_chart(fig1, use_container_width=True)

st.header("2. Top 10 des postes les mieux rémunérés par industrie")
df_salary = run_query(session, query_top10_salary)
fig2 = px.bar(df_salary, x="TITLE", y="MED_SALARY", color="industry_id",
              title="Top 10 des postes les mieux rémunérés par industrie",
              labels={"TITLE": "Titre de poste", "MED_SALARY": "Salaire médian", "INDUSTRY_ID": "ID Industrie"},
              barmode='group')
st.plotly_chart(fig2, use_container_width=True)

st.header("3. Répartition des offres d’emploi par taille d’entreprise")
df_company_size = run_query(session, query_offers_by_company_size)
fig3 = px.bar(df_company_size, x="company_size", y="NB_OFFRES",
              title="Répartition des offres d’emploi par taille d’entreprise",
              labels={"company_size": "Taille d’entreprise", "NB_OFFRES": "Nombre d’offres"})
st.plotly_chart(fig3, use_container_width=True)

st.header("4. Répartition des offres d’emploi par secteur d’activité")
df_industry = run_query(session, query_offers_by_industry)
fig4 = px.bar(df_industry, x="industry_id", y="NB_OFFRES",
              title="Répartition des offres d’emploi par secteur d’activité",
              labels={"INDUSTRY_ID": "ID Industrie", "NB_OFFRES": "Nombre d’offres"})
st.plotly_chart(fig4, use_container_width=True)

st.header("5. Répartition des offres d’emploi par type d’emploi")
df_work_type = run_query(session, query_offers_by_work_type)
# ici aussi colonne 'TYPE_EMPLOI' car alias SQL en majuscules
fig5 = px.pie(df_work_type, values="NB_OFFRES", names="TYPE_EMPLOI",
              title="Répartition des offres d’emploi par type d’emploi")
st.plotly_chart(fig5, use_container_width=True)
