-- 🎯 SETTING UP THE ENVIRONMENT

-- Assign the highest-level Snowflake role
USE ROLE ACCOUNTADMIN;

-- Use the available compute warehouse
USE WAREHOUSE COMPUTE_WH;

-- Create project database and schema if they don't already exist
CREATE DATABASE IF NOT EXISTS Portfolio_Project;
CREATE SCHEMA IF NOT EXISTS Portfolio_Project_sch;

-- Set the context to the project database and schema
USE DATABASE Portfolio_Project;
USE SCHEMA Portfolio_Project_sch;

-- 📦 STEP 1: CREATING BASE TABLES

-- Table 1: COVID Deaths dataset
CREATE OR REPLACE TABLE covid_deaths (
    iso_code STRING,
    continent STRING,
    location STRING,
    date DATE,
    population FLOAT,
    total_cases FLOAT,
    new_cases FLOAT,
    new_cases_smoothed FLOAT,
    total_deaths FLOAT,
    new_deaths FLOAT,
    new_deaths_smoothed FLOAT,
    total_cases_per_million FLOAT,
    new_cases_per_million FLOAT,
    new_cases_smoothed_per_million FLOAT,
    total_deaths_per_million FLOAT,
    new_deaths_per_million FLOAT,
    new_deaths_smoothed_per_million FLOAT,
    reproduction_rate FLOAT,
    icu_patients FLOAT,
    icu_patients_per_million FLOAT,
    hosp_patients FLOAT,
    hosp_patients_per_million FLOAT,
    weekly_icu_admissions FLOAT,
    weekly_icu_admissions_per_million FLOAT,
    weekly_hosp_admissions FLOAT,
    weekly_hosp_admissions_per_million FLOAT,
    total_tests FLOAT,
    new_tests FLOAT,
    total_tests_per_thousand FLOAT,
    new_tests_per_thousand FLOAT,
    new_tests_smoothed FLOAT,
    new_tests_smoothed_per_thousand FLOAT,
    positive_rate FLOAT,
    tests_per_case FLOAT,
    tests_units STRING,
    total_vaccinations FLOAT,
    people_vaccinated FLOAT,
    people_fully_vaccinated FLOAT,
    new_vaccinations FLOAT,
    new_vaccinations_smoothed FLOAT,
    total_vaccinations_per_hundred FLOAT,
    people_vaccinated_per_hundred FLOAT,
    people_fully_vaccinated_per_hundred FLOAT,
    new_vaccinations_smoothed_per_million FLOAT,
    stringency_index FLOAT,
    population_density FLOAT,
    median_age FLOAT,
    aged_65_older FLOAT,
    aged_70_older FLOAT,
    gdp_per_capita FLOAT,
    extreme_poverty FLOAT,
    cardiovasc_death_rate FLOAT,
    diabetes_prevalence FLOAT,
    female_smokers FLOAT,
    male_smokers FLOAT,
    handwashing_facilities FLOAT,
    hospital_beds_per_thousand FLOAT,
    life_expectancy FLOAT,
    human_development_index FLOAT
);

-- Table 2: COVID Vaccinations dataset
CREATE OR REPLACE TABLE covid_vaccinations (
    iso_code STRING,
    continent STRING,
    location STRING,
    date DATE,
    population FLOAT,
    total_cases FLOAT,
    new_cases FLOAT,
    new_cases_smoothed FLOAT,
    total_deaths FLOAT,
    new_deaths FLOAT,
    new_deaths_smoothed FLOAT,
    total_cases_per_million FLOAT,
    new_cases_per_million FLOAT,
    new_cases_smoothed_per_million FLOAT,
    total_deaths_per_million FLOAT,
    new_deaths_per_million FLOAT,
    new_deaths_smoothed_per_million FLOAT,
    reproduction_rate FLOAT,
    icu_patients FLOAT,
    icu_patients_per_million FLOAT,
    hosp_patients FLOAT,
    hosp_patients_per_million FLOAT,
    weekly_icu_admissions FLOAT,
    weekly_icu_admissions_per_million FLOAT,
    weekly_hosp_admissions FLOAT,
    weekly_hosp_admissions_per_million FLOAT,
    total_tests FLOAT,
    new_tests FLOAT,
    total_tests_per_thousand FLOAT,
    new_tests_per_thousand FLOAT,
    new_tests_smoothed FLOAT,
    new_tests_smoothed_per_thousand FLOAT,
    positive_rate FLOAT,
    tests_per_case FLOAT,
    tests_units STRING,
    total_vaccinations FLOAT,
    people_vaccinated FLOAT,
    people_fully_vaccinated FLOAT,
    new_vaccinations FLOAT,
    new_vaccinations_smoothed FLOAT,
    total_vaccinations_per_hundred FLOAT,
    people_vaccinated_per_hundred FLOAT,
    people_fully_vaccinated_per_hundred FLOAT,
    new_vaccinations_smoothed_per_million FLOAT,
    stringency_index FLOAT,
    population_density FLOAT,
    median_age FLOAT,
    aged_65_older FLOAT,
    aged_70_older FLOAT,
    gdp_per_capita FLOAT,
    extreme_poverty FLOAT,
    cardiovasc_death_rate FLOAT,
    diabetes_prevalence FLOAT,
    female_smokers FLOAT,
    male_smokers FLOAT,
    handwashing_facilities FLOAT,
    hospital_beds_per_thousand FLOAT,
    life_expectancy FLOAT,
    human_development_index FLOAT
);

-- ✅ STEP 2: ANALYSIS QUERIES

-- 🧮 A. Total Cases vs. Total Deaths (Death Percentage)
SELECT 
    location,
    date,
    total_cases,
    new_cases,
    total_deaths,
    population,
    (total_deaths / total_cases) * 100 AS death_percentage
FROM covid_deaths
WHERE total_cases IS NOT NULL AND total_cases > 0;

-- 🧪 B. Filtered for United States
SELECT 
    location,
    date,
    total_cases,
    new_cases,
    total_deaths,
    population,
    (total_deaths / total_cases) * 100 AS death_percentage
FROM covid_deaths
WHERE location LIKE '%States%';

-- 🌍 C. Total Cases vs. Population (Infection Rate)
SELECT 
    location,
    date,
    total_cases,
    new_cases,
    total_deaths,
    population,
    (total_cases / population) * 100 AS infection_rate
FROM covid_deaths
WHERE location LIKE '%States%';

-- 🧾 D. Countries with Highest Infection Rate
SELECT 
    location,
    population,
    MAX(total_cases) AS highest_infection,
    MAX((total_cases / population) * 100) AS percentage_population_infected  
FROM covid_deaths
WHERE total_cases IS NOT NULL AND population IS NOT NULL AND population > 0
GROUP BY location, population
ORDER BY percentage_population_infected DESC;

-- 💀 E. Countries with Highest Total Deaths
SELECT 
    location, 
    MAX(total_deaths) AS total_death_count
FROM covid_deaths
WHERE total_deaths IS NOT NULL AND continent IS NOT NULL
GROUP BY location
ORDER BY total_death_count DESC;

-- 🌐 F. Total Deaths by Continent
SELECT 
    continent, 
    MAX(total_deaths) AS total_death_count
FROM covid_deaths
WHERE total_deaths IS NOT NULL AND continent IS NOT NULL
GROUP BY continent
ORDER BY total_death_count DESC;

-- 🌎 G. Global Aggregated Death Percentage by Date
SELECT 
    date,
    SUM(new_cases) AS total_new_cases,
    SUM(new_deaths) AS total_new_deaths,
    (SUM(new_deaths) / SUM(new_cases)) * 100 AS death_percentage
FROM covid_deaths
WHERE continent IS NOT NULL
GROUP BY date
ORDER BY date;

-- 💉 H. Total Vaccinations vs. Population (With CTE)
WITH vaccinations_cte AS (
    SELECT 
        cd.continent,
        cd.location,
        cd.population,
        cd.date,
        cv.new_vaccinations,
        SUM(cv.new_vaccinations) OVER (
            PARTITION BY cd.location 
            ORDER BY cd.date
        ) AS cumm_vacc
    FROM covid_deaths AS cd
    JOIN covid_vaccinations AS cv 
        ON cd.location = cv.location 
        AND cd.date = cv.date
    WHERE cd.continent IS NOT NULL
)

SELECT *,
       (cumm_vacc / population) * 100 AS percent_vaccinated
FROM vaccinations_cte
WHERE population IS NOT NULL;

-- 🧾 I. Store Result into Table for Reuse
CREATE OR REPLACE TABLE vaccination_summary_v1 AS
WITH vaccinations_cte AS (
    SELECT 
        cd.continent,
        cd.location,
        cd.population,
        cd.date,
        cv.new_vaccinations,
        SUM(cv.new_vaccinations) OVER (
            PARTITION BY cd.location 
            ORDER BY cd.date
        ) AS cumm_vacc
    FROM covid_deaths AS cd
    JOIN covid_vaccinations AS cv 
        ON cd.location = cv.location 
        AND cd.date = cv.date
    WHERE cd.continent IS NOT NULL
)
SELECT *,
       (cumm_vacc / population) * 100 AS percent_vaccinated
FROM vaccinations_cte
WHERE population IS NOT NULL;

-- 🎯 Final Result: Countries with >70% Population Vaccinated
SELECT * 
FROM vaccination_summary_v1
WHERE percent_vaccinated > 70
ORDER BY percent_vaccinated DESC;

