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

yes