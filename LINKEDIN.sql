create or replace database linkedin;   -- Création de la base de données linkedin

create or replace stage stage_linkedin url = 's3://snowflake-lab-bucket/';  -- Création du stage externe

list @stage_linkedin;  -- Liste des fichiers présents dans le stage

SELECT $1, $2, $3                    -- Tentatives pour voir le contenu brut des fichiers .csv selon le nombre de colonnes 
FROM @stage_linkedin/benefits.csv;


SELECT $1, $2, $3, $4             
FROM @stage_linkedin/employee_counts.csv;


/*SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24 ,$25, $26, $27          
FROM @stage_linkedin/job_postings.csv;*/


SELECT $1, $2
FROM @stage_linkedin/job_skills.csv;


SELECT $1                                -- ~~~~~~
FROM @stage_linkedin/companies.json;

-- Définition des formats de fichiers

CREATE or replace FILE FORMAT format_csv    -- Format CSV
TYPE = 'CSV' 
FIELD_DELIMITER = ',' 
RECORD_DELIMITER = '\n' 
SKIP_HEADER = 1
field_optionally_enclosed_by = '\042'
null_if = ('');


CREATE or REPLACE file format format_json    -- Format JSON
type = 'JSON'
STRIP_OUTER_ARRAY=TRUE;

-- Création des tables

create or replace table benefits   -- Table benefits
(
   job_id    int,
   inferred	 boolean,
   typee     varchar
);

create or replace table companies_json  -- Table companies_json
(
    data variant
);

create or replace table company_industries_json  -- Table company_industries_json
(
   data variant
);

create or replace table company_specialities_json   -- Table company_specialities_json
(
  data variant
);

create or replace table employee_counts  --  Table employee_counts
(
   company_id        int,
   employee_count    int,
   follower_count    int,
   time_recorded     float
);

create or replace table job_industries_json  -- Table job_industries_json
(
   data variant
);

create or replace table job_postings  -- Table job_postings
(
   job_id                      int,
   company_id	               int,
   title                       varchar,
   description                 varchar,
   max_salary	               float,
   med_salary                  float,
   min_salary                  float,
   pay_period	               string,
   formatted_work_type	       string,
   locationn                   varchar,       -- location mot réservé de Snowflake donc renommé 
   applies                     int,
   original_listed_time	       varchar,
   remote_allowed		       varchar,
   viewss                      int,           -- views mot réservé de Snowflake donc renommé 
   job_posting_url             varchar,
   application_url	           varchar,
   application_type	           varchar,
   expiry		               varchar,
   closed_time                 varchar,
   formatted_experience_level  varchar,
   skills_desc                 varchar,
   listed_time                 varchar,
   posting_domain	           varchar,
   sponsored	               boolean,
   work_type	               varchar,
   currency                    string,
   compensation_type           varchar
);

create or replace table job_skills  -- Table  job_skills
(
   job_id     int,
   skill_abr  string
);


-- Chargement des données

copy into benefits from @stage_linkedin/benefits.csv  file_format= format_csv; --Chargement depuis le fichier benefits.csv dans la table benefits créée

select * from benefits;  --Contenu de la table benefits , toutes les données ont été bien chargées


copy into companies_json from @stage_linkedin/companies.json  file_format= format_json;
 
select * from companies_json;


copy into company_industries_json from @stage_linkedin/company_industries.json  file_format= format_json;

select * from company_industries_json;


copy into company_specialities_json from @stage_linkedin/company_specialities.json  file_format= format_json;

select * from company_specialities_json;


copy into employee_counts from @stage_linkedin/employee_counts.csv  file_format= format_csv; 

select * from employee_counts;


copy into job_industries_json from @stage_linkedin/job_industries.json  file_format= format_json;

select * from job_industries_json;


copy into job_postings from @stage_linkedin/job_postings.csv  file_format= format_csv;  

select * from job_postings;


copy into job_skills from @stage_linkedin/job_skills.csv  file_format= format_csv; 

select * from job_skills;


-- Transformations nécessaires  -- Transformations sur les tables JSON pour avoir un exemple de tables relationnelles
-- Nous allons créer des vues pour une utilisaton plus souple pour les requêtes à venir

create or replace view companies as                          -- companies
select data:address::varchar       as "address",
       data:city::varchar          as "city",
       data:company_id::int        as "company_id",
       data:company_size::int      as "company_size",
       data:country::varchar       as "country",
       data:description::varchar   as "description",
       data:name::varchar          as "name",
       data:state::varchar         as "state",
       data:url::varchar           as "url",
       data:zip_code::varchar      as "zip_code"
from companies_json;

select * from companies;  -- La vue a été bien créée avec toutes les données


create or replace view company_industries  as                        -- company_industries
select data:company_id::int     as "company_id",
       data:industry::varchar   as "industry"
from company_industries_json;

select * from company_industries;


create or replace view company_specialities as                       -- company_specialities
select data:company_id::int         as "company_id",
       data:speciality::varchar     as "speciality"
from company_specialities_json;

select * from company_specialities;


create or replace view job_industries as                       -- job_industries
select data:industry_id::int  as "industry_id",
       data:job_id::int       as "job_id"
from job_industries_json;

select * from job_industries;


-- Analyse des Données

-- 1/ Top 10 des titres de postes les plus publiés par industrie.

SELECT ji."industry_id", jp.title, COUNT(*) AS nb_publications    -- COUNT(*) : nombre de fois où ce titre est publié par cette industrie.
FROM job_postings jp JOIN job_industries ji
ON jp.job_id = ji."job_id"
GROUP BY ji."industry_id", jp.title
QUALIFY  ROW_NUMBER() OVER (PARTITION BY ji."industry_id" ORDER BY COUNT(*) DESC) <= 10  --  pour garder que les 10 titres les plus fréquents par industrie.
ORDER BY ji."industry_id", nb_publications DESC;


-- 2/ Top 10 des postes les mieux rémunérés par industrie.

SELECT ji."industry_id", jp.title, jp.med_salary
FROM job_postings jp JOIN job_industries ji 
ON jp.job_id = ji."job_id"
WHERE jp.med_salary IS NOT NULL          -- IS NOT NULL pour que l'analyse soit pertinente
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY ji."industry_id"
    ORDER BY jp.med_salary DESC
  ) <= 10
ORDER BY ji."industry_id", jp.med_salary DESC;


-- 3/ Répartition des offres d’emploi par taille d’entreprise.

SELECT  c."company_size", COUNT(*) AS nb_offres
FROM job_postings jp JOIN  companies c 
ON jp.company_id = c."company_id"
GROUP BY  c."company_size"
ORDER BY  c."company_size";

-- 4/ Répartition des offres d’emploi par secteur d’activité.

SELECT ji."industry_id", COUNT(*) AS nb_offres
FROM job_postings jp JOIN   job_industries ji 
ON jp.job_id = ji."job_id"
GROUP BY ji."industry_id"
ORDER BY nb_offres DESC;

-- 5/ Répartition des offres d’emploi par type d’emploi (temps plein, stage, temps partiel).

SELECT jp.formatted_work_type AS type_emploi, COUNT(*) AS nb_offres
FROM job_postings jp
GROUP BY jp.formatted_work_type
ORDER BY nb_offres DESC;























































































