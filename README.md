

## Model Scoring High Level User Manual
### Steps to Run the Scoring Pipeline Code on Local Environments Before Deploying the Code on Airflow




---
Preparation Work :
- Add folder where most of the files are saved to pythonpath in ~/home/.bashrc 
Example: `export PYTHONPATH="$PYTHONPATH:/home/jupyter/projects/datahub-airflow/dags/fintech`

- Change the directory :
`cd dags/fintech`


### Model Folder Structure for Talabat Automatic Scoring:
- Focus on folder: `dags/fintech/data_science/talabat/projects/post_paid/ae/auto_decisioning/auto_scoring`

- Folder structure:

      - region: ae, `another-region`
        - model_type: a_score, b_score
          - scope_name: generic, new_user, low_frequency, `another-scope`


### **Step 0** - Prepare a requirements.txt file and put it under:
`dags/fintech/data_science/talabat/projects/post_paid/region/auto_decisioning/auto_scoring/model_type/scope_name`

- Example path: `dags/fintech/data_science/talabat/projects/post_paid/ae/auto_decisioning/auto_scoring/a_score/generic/requirements.txt`

### **Step 1** - Install the Virtual Environment:

- Use `dags/fintech/data_science/talabat/projects/post_paid/ae/auto_decisioning/auto_scoring/Makefile` to install the virtual environment.

- Example usage :
- `make project_name=post_paid region=ae model_type=a_score scope_name=low_frequency -C ./data_science/talabat/projects/post_paid/ae/auto_decisioning/auto_scoring install_venv`
- This code installs `venv` under `dags/fintech/data_science/talabat/projects/post_paid/ae/auto_decisioning/auto_scoring/a_score/low_frequency/` folder

### **Step 2** - Install the Dependencies:

- Example usage :
  - `make project_name=post_paid region=ae model_type=a_score scope_name=low_frequency -C ./data_science/talabat/projects/post_paid/ae/auto_decisioning/auto_scoring install_dependencies`

###  **Step 3** - Update Your Code:
- Update the following files under your project folder:
  - configuration.py
  - scoring.py
  - main.py
  - and if needed put needed queries under queries folder

- Example files are under: `dags/fintech/data_science/talabat/projects/post_paid/ae/auto_decisioning/auto_scoring/a_score/low_frequency/`

###  **Step 4** - Run Scoring:

**Make sure** self.RUN_LOCAL_ENV is set to `True` in `configuration.py` file under your model project folder

- Example : `self.RUN_LOCAL_ENV=True` in dags/fintech/data_science/talabat_auto_scoring/post_paid/ae/a_score/low_frequency/configuration.py

`Warning!:` Dont use any production tables in your code at this step unless you think you are ready to deploy to airflow production environment.

- Example usage :
  - `make project_name=post_paid region=ae model_type=a_score scope_name=low_frequency -C ./data_science/talabat/projects/post_paid/ae/auto_decisioning/auto_scoring score metrics_ref_date="2024-07-20"`


###  **Step 5** - Prepare Dag:

Update the `dag_tasks.py` file under `dags/fintech/data_science/talabat/projects/post_paid/{region}/auto_decisioning/auto_scoring/dag` by <ins>adding<ins> new scoring steps.

`Warning!:` Don't update existing steps as the automatic scoring takes place for all existing models at once. This means there is only a single dag for scoring all postpaid models. <ins>Unless strictly needed,<ins> dont touch to existing dag steps in `dag_tasks.py`

Update the `dag.py` file by changing `version` and `change_log` variables if needed.

### **Step 6** - Check if Your Code works on Airflow Development Environment

**Make sure ---->**:

1. self.RUN_LOCAL_ENV is set to `False`  in `configuration.py` file under your model project folder

  - Example : `self.RUN_LOCAL_ENV=False` in dags/fintech/data_science/talabat/projects/post_paid/ae/auto_decisioning/auto_scoring/a_score/low_frequency/configuration.py


Push your branch to Airflow development environment and run the pipeline through there.
Please know that you can trigger your dag with a configuration.
Example:
```
{
    "metrics_ref_date": "2024-08-28"
}
```

## Once the Code is Production Ready:

### **Step 7** - Check All Dag Steps are Active
Make sure  `self.RUN_LOCAL_ENV=False`  in `configuration.py` file under your model project folder.

### **Step 8** - Check Linters

- Example usage :
  - `make project_name=post_paid region=ae model_type=a_score scope_name=low_frequency -C ./data_science/talabat/projects/post_paid/ae/auto_decisioning/auto_scoring check`
`

### **Step 9** - Create a Pull Request and Deploy Branch on Airflow Production Environment.

Please know that you can trigger your dag with a configuration.
Example:
```
{
    "metrics_ref_date": "2024-08-28"
}
```









