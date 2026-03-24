# 🎵 Spotify Data Engineering (2017 - 2024)

This project focuses on Spotify data engineering, designed to analyze music data from 2017 to 2024. Through automated data pipelines, I clean, process, and extract valuable insights from music datasets.

---

## ⚠️ Important Security Notice

For security reasons, the `cloud` folder containing credential keys is hidden/ignored and will **NOT** be cloned with this repository. 

**Before starting the project, please complete the following steps:**
1. Manually create a folder named `cloud`.
2. Request the specific **JSON Token Key** from the Project Owner.
3. Place the acquired token key file inside the `cloud` folder you just created.

---

## 📂 Project Structure

The overall directory structure is organized as follows:

- **`sp_project/`**
  - **`config/`**: System configuration files
  - **`dags/`**: Airflow Directed Acyclic Graphs (DAGs)
  - **`cloud/`**: Stores external cloud service token keys
  - **`logs/`**: System and task execution logs
  - **`plugins/`**: Custom Airflow plugins and utilities

---

## 🚀 Getting Started

### Prerequisites
Ensure you have Docker installed and running. Also, make sure you have completed the "Important Security Notice" steps above.

### Step 1: Initialize the Environment
Open your terminal, navigate to the folder of the project, and run the following command to initialize the Airflow environment:

```bash
docker compose up airflow-init
```
> **💡 Expected Outcome**: This step initializes the database. Please ensure the terminal indicates that both redis and postgres services have started successfully without errors.

### Step 2: Start All Services
Once initialization is complete, run the following command to start all remaining services in the background:

```bash
docker compose up -d
```
> **💡 Expected Outcome**: All containers will start. You can open Docker to verify that all services are up, running, and Healthy. If any service fails to start properly, please attempt to restart it.
