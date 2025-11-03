# Notebooks Directory

This directory contains Jupyter notebooks for data exploration, analysis, and pipeline testing.

## Contents

### 1. data_exploration.ipynb
- Exploratory Data Analysis (EDA) of retail transaction data
- Data quality assessment and visualization
- Statistical analysis of sales patterns
- Customer and product insights

### 2. pipeline_testing.ipynb
- ETL pipeline testing and validation
- Performance benchmarking
- Data transformation verification
- Output validation

## Usage

To use these notebooks:

1. Install Jupyter:
```bash
pip install jupyter
```

2. Launch Jupyter Notebook:
```bash
jupyter notebook
```

3. Open the desired notebook from the Jupyter interface

## Requirements

All dependencies are listed in the main `requirements.txt` file. Additionally, you may need:
- matplotlib
- seaborn
- plotly

Install visualization libraries:
```bash
pip install matplotlib seaborn plotly
```

## Note

These notebooks are for exploration and testing purposes. For production ETL runs, use the main pipeline script in the `src/` directory.
