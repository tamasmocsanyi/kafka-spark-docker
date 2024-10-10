# Project Summary

This project implements a real-time data pipeline for streaming and processing GPS coordinates using a combination of well-established, scalable technologies:

- **Docker** for containerization, simplifying environment setup and deployment.

- **Apache Airflow** for task scheduling and orchestration.

- **Apache Kafka** for real-time message streaming.
    
- **Apache Spark** for distributed data processing.    

- **Grafana** for monitoring and visualization.
    

The pipeline ingests GPS data from Kafka topics, processes it with PySpark, and stores the results in a PostgreSQL database. Airflow automates and manages the entire workflow, ensuring smooth coordination of each component. All services run in Docker containers for easy setup and portability.