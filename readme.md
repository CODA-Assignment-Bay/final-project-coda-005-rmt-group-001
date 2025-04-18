
# Analysis of Seoul Air Pollution in 2017-2019 

Determining strategies to tackle air pollution in Seoul by understanding the city's air conditions through an analysis of air pollution data from 2017 to 2019, in order to identify the causes of poor air quality in Seoul. The analysis will be conducted over the course of one week.


## Introduction
Air pollution poses serious environmental and health risks such as respiratory deasese, cancer & acid rain.

Seoul faces persistent air pollution issues due to a mix of local and external factors. Heavy traffic, industrial activities, and high population density contribute in pollution levels.

In addition, fine dust (PM 10) from neighboring countries, especially during certain seasons, worsens the problem. The city regularly experiences days with unhealthy air quality, affecting public health and daily life.


## Problems Statement

Determining strategies to tackle air pollution in Seoul by understanding the city's air conditions through an analysis of air pollution data from 2017 to 2019, in order to identify poor air quality factors in Seoul. The analysis will be conducted over the course of one week.


## Data Engineering
In this process, data from source ("https://www.kaggle.com/datasets/bappekim/air-pollution-in-seoul") divided into data models.

Then ETL process using DAG airflow into neon database ("postgresql://neondb_owner:npg_f1uYCZ8nRWzQ@ep-floral-lake-a533zre9-pooler.us-east-2.aws.neon.tech/Air-Pollution-Seoul?sslmode=require").

After that, ETL process again ("postgresql://neondb_owner:npg_f1uYCZ8nRWzQ@ep-floral-lake-a533zre9-pooler.us-east-2.aws.neon.tech/Air-Pollution-Seoul?sslmode=require") to become a datamart that will be utilized by Data Analyst Team


## Data Analysis
In this process, datamart utilized for finding insight regarding problems statement https://public.tableau.com/views/FPrev1/Dashboard1?:language=en-US&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link
