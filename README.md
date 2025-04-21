# Mitsui MLOps Pipeline with Azure DevOps and Snowflake

This project demonstrates a production-ready Machine Learning Operations (MLOps) pipeline that integrates Azure DevOps with Snowflake. The pipeline automates the entire ML lifecycle from feature engineering to model deployment.

## 🏗️ Architecture

```
mlops/
├── models/         # ML model definitions and training code
├── features/       # Feature engineering and feature store code
├── tasks/         # Automated Snowflake tasks for model execution
├── helpers.py     # Utility functions
├── snowflake_queries.py  # Database interaction code
└── azure-pipelines.yml   # CI/CD pipeline definition
```

## 🚀 Features

- Automated CI/CD pipeline using Azure DevOps
- Feature engineering and Feature Store implementation in Snowflake
- Secure credential management and authentication
- Automated model training and deployment
- Data validation and testing
- Production-ready ML model serving

## 📋 Prerequisites

- Azure DevOps account with appropriate permissions
- Snowflake account with:
  - ACCOUNTADMIN or SECURITYADMIN role for initial setup
  - Warehouse, Database, and Schema creation privileges
- Python 3.8 or higher
- Private key for Snowflake authentication

## 🔧 Setup

1. **Snowflake Configuration**
   ```sql
   -- Create required Snowflake objects
   CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;
   CREATE DATABASE IF NOT EXISTS XYZ_DEV;
   CREATE SCHEMA IF NOT EXISTS FEATURE_STORE;
   ```

2. **Azure DevOps Configuration**
   - Create a variable group named `snowflake-credentials` with:
     - `account`: Your Snowflake account identifier
     - `user`: Snowflake username
     - `warehouse`: Snowflake warehouse name
     - `role`: Snowflake role
     - `database`: Snowflake database name
     - `schema`: Snowflake schema name
     - `private_key_file_name`: Name of your private key file

3. **Local Development Setup**
   ```bash
   # Create virtual environment
   python -m venv venv_mitsui_py310
   source venv_xyz_py310/bin/activate  # Unix/macOS
   # or
   .\venv_xyz_py310\Scripts\activate  # Windows

   # Install dependencies
   pip install -r requirements.txt
   ```

## 🏃‍♂️ Running the Pipeline

The pipeline is automatically triggered on pushes to:
- main
- snowflakeconnection
- agemodel
- tasks

Pipeline stages:
1. Python Setup
2. Private Key Download
3. Snowflake Connection Test
4. Feature Building
5. Model Building
6. Task Creation

## 📊 Feature Engineering

Features are built using Snowflake's Feature Store functionality. Current features include:
- Patient age calculation
- Additional features can be added by creating new feature engineering scripts in the `features/` directory

Example of adding a new feature:
```python
from snowflake.ml.feature_store import FeatureStore, Entity, FeatureView

# Create feature logic
def create_new_feature():
    # Your feature engineering code here
    pass

# Register in feature store
fs = FeatureStore(...)
feature_view = FeatureView(...)
fs.register_feature_view(feature_view)
```

## 🤖 Model Development

Models are stored in the `models/` directory. To add a new model:
1. Create a new Python file in `models/`
2. Implement model training logic
3. Add model registration code
4. Update `azure-pipelines.yml` to include the new model

## 🔐 Security

- Sensitive credentials are stored in Azure DevOps variable groups
- Snowflake connection uses key pair authentication
- All secrets are managed through secure environment variables

## 🧪 Testing

To run tests locally:
```bash
python test_connection.py  # Test Snowflake connectivity
```

## 📝 Contributing

1. Create a new branch
2. Make your changes
3. Submit a pull request
4. Ensure CI/CD pipeline passes

## 🚨 Troubleshooting

Common issues and solutions:
- **Snowflake Connection Issues**: Verify credentials and private key
- **Pipeline Failures**: Check Azure DevOps logs
- **Feature Store Errors**: Verify Snowflake permissions

## 📚 Additional Resources

- [Snowflake Documentation](https://docs.snowflake.com/)
- [Azure DevOps Documentation](https://docs.microsoft.com/en-us/azure/devops/)
- [MLOps Best Practices](https://docs.microsoft.com/en-us/azure/machine-learning/concept-model-management-and-deployment)
