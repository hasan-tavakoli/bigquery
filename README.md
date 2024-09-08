# SQLBigQueryDataSource

This repository contains the `SQLBigQueryDataSource` class, which provides an interface for interacting with Google BigQuery using SQLAlchemy. The class supports executing queries, creating tables with specified schemas, and deleting tables.

## Overview

The `SQLBigQueryDataSource` class provides methods to:

- Execute SQL queries on BigQuery.
- Retrieve existing tables.
- Create new tables with specified schemas.
- Delete tables from BigQuery.

## Requirements

- Python 3.x
- SQLAlchemy
- pandas
- Google Cloud BigQuery client library
- Google Cloud credentials

## Installation

Install the necessary Python packages using pip:

```bash
pip install sqlalchemy pandas google-cloud-bigquery
