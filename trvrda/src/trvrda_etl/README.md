# trvrda

This folder defines all source code for the trvrda pipeline:

- `explorations/`: Ad-hoc notebooks used to explore the data processed by this pipeline.
- `transformations/`: All dataset definitions and transformations.
- `utilities/` (optional): Utility functions and Python modules used in this pipeline.
- `data_sources/` (optional): View definitions describing the source data for this pipeline.

## Getting Started

To get started, go to the `transformations` folder -- most of the relevant source code lives there:

* By convention, every dataset under `transformations` is in a separate file.
* Take a look at the sample called "sample_trips_trvrda.py" to get familiar with the syntax.
  Read more about the syntax at https://docs.databricks.com/dlt/python-ref.html.
* If you're using the workspace UI, use `Run file` to run and preview a single transformation.
* If you're using the CLI, use `databricks bundle run trvrda_etl --select sample_trips_trvrda` to run a single transformation.

For more tutorials and reference material, see https://docs.databricks.com/dlt.


# Telangana-RTA-Vehicle-Registration-Data-Analysis-

## Project Folder Structure

```
Telangana-RTA-Vehicle-Registration-Data-Analysis-
├── README.md
├── requirements.txt
├── tests/
│   └── test_trvrda.py
├── .github/
│   └── workflows/
│       └── ci.yml
├── trvrda/
│   ├── .gitignore
│   ├── databricks.yml
│   ├── pyproject.toml
│   ├── README.md
│   ├── .databricks/
│   │   ├── .gitignore
│   │   └── bundle/
│   │       └── dev/
│   │           ├── deployment.json
│   │           ├── bin/
│   │           │   └── terraform
│   │           ├── terraform/
│   │           │   ├── terraform.tfstate
│   │           │   ├── terraform.tfstate.backup
│   │           │   ├── plan
│   │           │   ├── .terraform.lock.hcl
│   │           │   └── bundle.tf.json
│   │           │   └── .terraform/
│   │           │       └── providers/
│   │           │           └── registry.terraform.io/
│   │           │               └── databricks/
│   │           │                   └── databricks/
│   │           │                       └── 1.106.0/
│   │           │                           └── linux_arm64/
│   │           │                               ├── terraform-provider-databricks_v1.106.0
│   │           │                               ├── NOTICE
│   │           │                               └── LICENSE
│   │           └── sync-snapshots/
│   │               └── 4a9cc7c81e81c003.json
│   ├── .vscode/
│   │   ├── extensions.json
│   │   ├── settings.json
│   │   └── __builtins__.pyi
│   ├── resources/
│   │   ├── piplines/
│   │   │   └── piplines.yml
│   │   └── variables/
│   │       └── variables.yml
│   └── src/
│       └── trvrda_etl/
│           ├── README.md
│           ├── transformations/
│           │   └── main.py
│           └── explorations/
│               ├── bronz.py
│               ├── gold.py
│               ├── silver.py
│               ├── main (1).py
│               ├── sample_job.job.yml
│               └── trvrda_etl.pipeline.yml
```

---


