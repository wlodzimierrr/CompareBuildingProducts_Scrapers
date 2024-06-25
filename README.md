# Building Products Comparison Website
## Overview
This project is a building products comparison website that enables users to compare prices and details from various hardware stores. The front end features a search bar and product categories for easy navigation.<br/> 
The back end features AWS EventBridge and AWS Lambda automate the initiation of scrapers, including the retrieval of the latest Docker image from AWS ECR, hosted on an AWS EC2 instance. They manage server tasks and perform weekly indexing with Algolia Search. Once these operations are complete, triggered by Python code, the AWS Lambda function then shuts down the EC2 instance and sends an email with Docker container logs from the operation.
Additionally, the system utilizes AWS SES to email logs and error notifications, ensuring administrators are kept up-to-date on the scraper's status and issues. The platform includes dedicated scrapers for stores like Tradepoint and Screwfix so far.
## Architecture
* **Front End:** Developed using Next.js, hosted separately, features an interactive UI with search functionality and product categorization.
* **Back End:** Python application hosted on AWS EC2, automated instance management using AWS Lambda and EventBridge.
* **Database:** PostgreSQL on AWS RDS with Algolia Search engine. (There was an attempt to create Qdrant vector database with sparse (BM25) and dense (clip-ViT-B-32) vector models using Pinecone BM25 Encoder and CLIP model from huggingface with good results. Hovewer, the dataset over 60000 images was too big to push here)
* Yes, I know pg_vector released sparse and dense vectors compability which I will test soon.
* **Data:** There are two databases: one for paths to scrape, which consists of different shops, and another for the products with normalised data from different shops."
* **Email Notifications:** Utilizes AWS SES for sending operational logs and error notifications.
* **Scrapers:** Python modules for extracting product data from various online stores.
* **CI/CD Pipeline:** Automated using GitHub Actions, builds and deploys Docker containers to AWS ECR.
## Repositories
* Front End: [CompareBuildingProducts_Web](https://github.com/wlodzimierrr/CompareBuildingProducts_Web)
* Back End: This repository.

![image](https://github.com/wlodzimierrr/CompareBuildingProducts_Scrapers/assets/140817588/5fd24cca-643c-4d3e-bf1b-8dadd8a26d7b)
  
![image](https://github.com/wlodzimierrr/CompareBuildingProducts_Scrapers/assets/140817588/cab490aa-11db-455b-a338-6903b55765e2)

