
# Introduction
This project focuses on analyzing the NYC Yellow Taxi data to gain insights and perform data-driven analytics. It involves data modeling using dbt (data build tool) with .sql scripts and creating a data dashboard using Power BI. The dataset contains information about taxi trips in New York City, including details such as pickup and dropoff locations, trip duration, fare amount, and more.


# Scope of Work
The scope of this project includes the following:

## 1) Data Modeling Design:
- For initial data modeling design, please refer to repo directory `./design/*`

## 2) Data Modeling:
Creation of three data models using dbt 
- `daily_received_amount_distribution`: This model focuses on analyzing the daily distribution of received amounts from taxi trips. 
![Image Alt Text](/datamodeling/data%20modeling%20-%20daily_received_amount_distribution.png){: width="50%"}
- `daily_taxi_revenue_and_opportunity_consol`: This model consolidates the daily taxi revenue and identifies potential business opportunities (pickup only).
- `daily_taxi_revenue`: This model analyzes the daily taxi revenue and identifies trends and patterns.
- For actual data modeling implementation, please refer to repo directory `./taxi_data_model/*`






## 3) Data Dashboard:
Creation of a Power BI dashboard to visualize and present key insights from the data models.
The dashboard will focus on two main domains:
- `Monthly Sales Revenue with Received Amount Distribution Trend`: This visualization will showcase the monthly sales revenue and highlight the trend in received amount distribution over time.
- `Taxi Pickup Business Opportunity with Trend`: This visualization will highlight the business opportunities for taxi pickups and showcase the trend in demand and potential areas of growth.


# Data Dashboard Presentation:
## The data dashboard will present the following information:

### Monthly Sales Revenue with Received Amount Distribution Trend:
- Visualizations showing the monthly sales revenue for taxi trips.
- Trend analysis of the received amount distribution over time.
- Comparison of revenue and received amount across different months.
- Insights into revenue growth and potential areas for improvement.
- Taxi Pickup Business Opportunity with Trend

### Visualizations showcasing the business opportunities for taxi pickups.
- Analysis of pickup locations with the highest demand.
- Trend analysis of pickup locations and identification of potential areas for business expansion.
- Insights into customer preferences and opportunities for revenue growth.

