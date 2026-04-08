# kalshi-new-scraping


## Overview
This project is designed to monitor live news information through the NYT API and map them to affected prediction markets on **Kalshi**. By identifying any semantic relationship between breaking news and active Kalshi event, the system aims  to identify market-moving developments before their prices are fully adjusted.

The pipeline utilizes **Airflow (Astro)** to run a DAG that scrapes Kalshi and NYT for events and keywords and loads data in **Snowflake (our data warehouse)**, and a **TF-IDF weighted matching algorithm (IN DEVELOPMENT)** to identify any related articles/events. On each DAG run, we plan to wipe all tables.

## Setup & Installation
To install astro, run the following command.

`brew install astro`

Afterwards, please clone the repository and create a .env file to add your Snowflake credentials. These will be picked up and used in the `airflow_settings.yaml` file. Remember place your private key inside of the `include/` directory. 

To start the environment, run the following command.

`astro dev start`

Now, you should be able to view the UI at http://localhost:8080.

