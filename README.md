**📊 SQL Data Warehouse Project (Medallion Architecture)**  

**📌 Project Overview**

This project demonstrates the end-to-end design and implementation of a modern SQL-based Data Warehouse using the Medallion Architecture (Bronze → Silver → Gold).  
- It covers:
- Data architecture design
- Data integration and transformation
- Data quality validation
- Dimensional modeling (Star Schema)
- Production-style SQL scripting and documentation
The goal is to transform raw operational data into analytics-ready datasets suitable for reporting and decision-making.

**🏗️ Architecture Overview**  
The solution follows the Medallion Architecture pattern:

**🥉 Bronze Layer (Raw Data)**
- Stores raw ingested data from CRM and ERP systems
- No transformation
- Preserves original source values for traceability

**🥈 Silver Layer (Cleansed & Conformed)**  
- Data cleansing and standardization
- Deduplication and validation
- Business rule enforcement
- Data type alignment to enable joins across systems

**🥇 Gold Layer (Analytics & Reporting)**
- Dimensional data modeling
- Star schema design
- Surrogate keys
- Fact and dimension tables optimized for BI tools

**🧩 Data Sources** <br>

**CRM System**
  - Customer information
  - Product details
  - Sales transactions

**ERP System**
  - Customer demographics
  - Location data
  - Product categories

**🔄 Data Flow**  
* Source systems load raw data into Bronze Layer

* Silver layer:
  - Cleans data  
  - Resolves data quality issues
  - Standardizes keys and attributes

* Gold layer:
  - Builds dimensional models
  - Creates fact–dimension relationships
  - Prepares data for analytics and reporting

**📁 Visual diagrams are included in the docs/diagrams/ folder:**
This folder has in it 2 copies of each diagram. One copy for the image view and the other copy as editing file
- Data Architecture
- Data Flow
- Integration Model
- Star Schema (Sales Data Mart)

**Data Quality & Validation**
Extensive data quality checks were implemented, including:
  - Null and duplicate key detection
  - Invalid date handling
  - Numeric validation (negative or zero values)
  - String standardization (trimming, casing)
  - Business rule consistency (e.g. Sales = Quantity × Price)
Quality checks are documented in the /Tests/ folder for the Silver & Gold layers.

**⚙️ ETL & Transformation Logic**  
**Stored Procedures**
  - Centralized ETL logic using SQL stored procedures
  - Truncation and reload strategy for Silver layer
  - Robust error handling with TRY…CATCH
  - Load duration tracking for operational transparency

**Key Transformations**
- Business key standardization (e.g. replacing '-' with '_')
- Gender and marital status normalization
- Date cleansing and validation
- Surrogate key preparation for Gold layer joins

**Dimensional Modeling (Gold Layer)**
The Gold layer implements a Star Schema, including:
  - Fact table for sales transactions
  - Dimension tables for customers, products, and categories
  - Surrogate keys for performance and consistency
  - Clean separation of facts and descriptive attributes

This structure is optimized for:
- BI dashboards 
- Aggregations 
- Time-based analysis
- Business reporting

**🛠️ Technologies Used**
- SQL Server (T-SQL)
- Medallion Architecture
- Dimensional Modeling (Star Schema)
- Git & GitHub (version control & documentation)
- Draw.io (architecture & data modeling diagrams)
- Notion: Documentation & Planning (project planning, task tracking, data catalog drafts)

**📁 Repository Structure**  
├── diagrams/<br>
│   ├── Data Architecture <br>
│   ├── Integration Model<br>
│   ├── Data Flow<br>
│   └── Star Schema<br>
│  
├── sql/  
│   ├── bronze/  
│   ├── silver/  
│   └── gold/  
│  
├── data_catalog.md  
└── README.md  

**Key Skills Demonstrated**
- Data Warehouse Architecture
- SQL ETL Development
- Data Quality Engineering
- Medallion Architecture
- Dimensional Modeling
- Production-ready SQL practices
- Technical documentation

**Outcome**
This project simulates a real-world enterprise data warehouse and demonstrates the ability to:
- Design scalable data architectures
- Handle messy real-life data
- Build analytics-ready datasets
- Document and version-control data solutions professionally

**📌 Next Enhancements (Optional)**
- Incremental loading strategy
- Audit & logging tables
- Performance indexing
- BI dashboard integration (Power BI / Tableau)

**👤 Author**

**Ignatus Dennis Acquah**
**BSc Business Administration (Finance & Banking)**
**Aspiring Data Analyst / Data Engineer**
