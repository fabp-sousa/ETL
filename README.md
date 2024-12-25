### **ETL Projects Repository**

This repository contains ETL (Extract, Transform, Load) pipelines designed to demonstrate data processing, transformation, and integration workflows using Python and relevant tools. Each project showcases a specific use case and employs best practices in data engineering.

---

### **Projects Overview**

#### **Project 1: Data Pipeline for Merging Two Companies**
- **Objective**: Build a data pipeline to merge datasets from two companies, process the data, and generate a clean, consolidated CSV file.
- **Technologies Used**: 
  - Python
  - Pandas
- **Key Features**:
  - Data cleaning and standardization to handle inconsistencies.
  - Application of transformation logic to unify the datasets.
  - Exported the final processed data as a CSV file for downstream use.
- **Outcome**: Delivered a ready-to-use, clean dataset as a CSV file, ensuring compatibility with analytical and visualization tools.

---

#### **Project 2: Recipe Data Processing with Polars and PostgreSQL**
- **Objective**: Extract recipe data, process it using Polars for high performance, and load it into a PostgreSQL database hosted on a Docker container.
- **Technologies Used**:
  - Python
  - Polars (for faster data processing)
  - PostgreSQL (for relational database storage)
  - Docker (for containerized database setup)
  - Database for connectivity and exploration
- **Key Features**:
  - **Data Extraction**: Recipe data was extracted from raw files from keggle.
  - **Data Transformation**:
    - High-performance data cleaning and transformation using Polars.
    - Ensured optimized memory usage and faster operations compared to traditional tools like Pandas.
  - **Database Integration**:
    - **Docker Setup**: 
      - Used Docker to containerize a PostgreSQL instance.
    - **Database Setup**:
      - Designed and created the PostgreSQL database schema.
      - Connected to the database using DBever for monitoring and manual query execution.
    - **Data Loading**:
      - Transformed data was loaded into the PostgreSQL database as relational tables.
---

### **Setup and Requirements**

#### **Prerequisites**
Before running the projects, ensure the following tools and libraries are installed:
- **Python** (3.8+ recommended)
- **Pip** (Package Installer for Python)
- **Docker** (for containerized PostgreSQL setup)
- **DBever** (for connecting and querying the database)

#### **Required Python Libraries**
Install the required dependencies using the following command:
```bash
pip install -r requirements.txt
```

#### **Docker Configuration**
To set up the PostgreSQL container:
1. Ensure Docker is installed and running on your machine.
2. Use the `docker-compose.yml` provided in the repository to spin up the PostgreSQL container:
   ```bash
   docker-compose up -d
   ```
3. Verify the container is running:
   ```bash
   docker ps
   ```

#### **Database Configuration**
- Update the `.yml` file with your database credentials.
- Use DBever or similar tools to connect to the PostgreSQL instance if needed.

---

### **How to Run the Projects**

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/fabp-sousa/ETL.git
   cd ETL
   ```

2. **Set Up the Environment**:
   Create and activate a Python virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Run Project 1**:
   Navigate to the project directory and execute the script:
   ```bash
   python pipeline_dados/scripts/fusao_mercado_fev.py
   ```

5. **Run Project 2**:
   Ensure Docker is running and the PostgreSQL container is up. Then, execute:
   ```bash
   python data_pipeline_polars/scripts/etl_food.py
   ```

---
### **Future Enhancements**
- Add automated testing for ETL pipelines.
- Implement CI/CD pipelines for data processing workflows.
- Explore cloud services for hosting and scaling ETL processes.

This repository is continually evolving to include more use cases and advanced data engineering practices. Stay tuned!
