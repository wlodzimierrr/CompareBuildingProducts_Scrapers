# Building Products Comparison Website
**Front End**: [CompareBuildingProducts_Web](https://github.com/wlodzimierrr/CompareBuildingProducts_Web)
## Overview

This project is a building products comparison website that enables users to compare prices and details from various hardware stores. The front end features a search bar and product categories for easy navigation. The back end is automated to efficiently manage data retrieval and server operations.

## Architecture

### Front End

- **Technology**: Developed using Next.js.
- **Hosting**: Hosted separately.
- **Features**: Interactive UI with search functionality and product categorization.

### Back End

- **Technology**: Docker container with Python application.
- **Hosting**: Hosted on AWS EC2.
- **Automation**: Managed using AWS Lambda and AWS EventBridge.
- **Database**: 
  - **Primary**: PostgreSQL on AWS RDS with Algolia Search engine.
  - **Secondary (Experimental)**: Qdrant vector database with sparse (BM25) and dense (clip-ViT-B-32) vector models.
- **Data Organization**: 
  - **Paths Database**: Contains paths to scrape from different shops.
  - **Products Database**: Normalized data from different shops.
  - **Experimental Data Management**: Working on the pipeline from sitemap to paths database.

### Scrapers

- **Stores**: Dedicated scrapers for Tradepoint, Screwfix, and Wickes.
- **Technology**: Python modules, with BeautifulSoup for Wickes scraping.
- **Concurrency**: Uses multithreading to run all scrapers concurrently.
- **History**: Initial versions used JavaScript with Puppeteer and Selenium in Python before transitioning to more efficient methods.

### CI/CD Pipeline

- **Automation**: Managed using GitHub Actions.
- **Containerization**: Builds and deploys Docker containers to AWS ECR.

### Notifications

- **Email**: Utilizes AWS SES for sending operational logs and error notifications.

### Telemetry

- **Logging**: 
  - **Promtail**: For Docker logging.
  - **cAdvisor**: For Docker container metrics.
- **Monitoring**: 
  - **Node Exporter**: For server metrics.
  - **Grafana and Prometheus**: For visualization and monitoring, hosted on another server.
- **Elastic IP**: AWS Elastic IP is used to maintain the same IP address even when the server is restarted.

### Alternate days Indexing

- **Scheduler**: AWS EventBridge triggers indexing.
- **Search Engine**: Data indexed with Algolia Search.
- **Instance Management**: 
  - Starts EC2 instance.
  - Retrieves the latest Docker image from AWS ECR.
  - Shuts down the instance after task completion.
  - Sends an email with Docker container logs.

### Upcoming Features

- **Data Processing Tools**: Additional tools for data processing will be uploaded shortly.

## Diagrams

1. **Architecture Diagram**:

![Architecture Diagram](https://github.com/wlodzimierrr/CompareBuildingProducts_Scrapers/assets/140817588/5fd24cca-643c-4d3e-bf1b-8dadd8a26d7b)

2. **Database Diagram**:

![Process Flow Diagram](https://github.com/wlodzimierrr/CompareBuildingProducts_Scrapers/assets/140817588/cab490aa-11db-455b-a338-6903b55765e2)
