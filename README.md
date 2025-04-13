# E-commerce Data Pipeline Project

## Overview
This project demonstrates a complete data engineering workflow by creating a synthetic e-commerce dataset and building an ETL (Extract, Transform, Load) pipeline to process and analyze it. It serves as a learning tool for data engineering concepts and provides a foundation for more advanced data processing systems.

## Features
- Synthetic data generation with realistic e-commerce patterns
- Complete ETL pipeline implementation
- Data transformation for business insights
- SQLite database integration
- Basic data analysis and querying
- Pipeline monitoring and logging

## Technologies
- Python 3.x
- Pandas & NumPy for data manipulation
- Faker for synthetic data generation
- SQLite for data storage
- Jupyter Notebooks for development and documentation

## Project Structure
```
ecommerce-data-pipeline/
│
├── notebooks/
│   ├── data_generator.ipynb        # Data generation notebook
│   └── pipeline.ipynb              # ETL pipeline implementation
│
├── sql/
│   └── analysis_queries.sql        # SQL queries for data analysis
│
├── tests/
│   ├── test_data_generator.py      # Tests for data generator
│   └── test_pipeline.py            # Tests for ETL pipeline
│
├── data/                           # Generated data (gitignored)
│   ├── customers.csv
│   ├── products.csv
│   ├── orders.csv
│   └── order_items.csv
│
├── ecommerce.db                    # SQLite database (gitignored)
├── pipeline_log.txt                # Pipeline execution logs
├── requirements.txt                # Project dependencies
└── README.md                       # Project documentation
```

## Installation

1. Clone this repository:
```
git clone https://github.com/yourusername/ecommerce-data-pipeline.git
cd ecommerce-data-pipeline
```

2. Create and activate a virtual environment (optional but recommended):
```
python -m venv venv
source venv/bin/activate  # On Windows, use: venv\Scripts\activate
```

3. Install the required dependencies:
```
pip install -r requirements.txt
```

## Usage

### Generate Synthetic Data
Run the data generator notebook to create the synthetic dataset:
```
jupyter notebook notebooks/data_generator.ipynb
```

### Run the ETL Pipeline
Execute the pipeline notebook to process the data:
```
jupyter notebook notebooks/pipeline.ipynb
```

### Analyze the Data
Open the SQLite database in DB Browser for SQLite or use the provided SQL queries:
```
# If you have sqlite3 command-line tools installed:
sqlite3 ecommerce.db < sql/analysis_queries.sql
```

## Pipeline Monitoring
The pipeline logs execution details and errors to `pipeline_log.txt`. Check this file for debugging and monitoring pipeline runs.

## Sample Results
After running the pipeline, you can query the database for insights like:
- Customer segmentation by spending behavior
- Product popularity and sales performance
- Order trends over time
- Monthly revenue analysis

## Future Improvements
- Add data quality validation
- Implement incremental loading
- Create a visualization dashboard
- Add parallel processing for larger datasets
- Migrate to a more robust database system
- Implement Apache Airflow for workflow orchestration

## License
MIT

## Contributing
Contributions, issues, and feature requests are welcome!

---

*This project was created as a learning exercise to demonstrate data engineering concepts without incurring costs.*
