# Telangana RTA Vehicle Registration Data Analysis

### 📺 Project Demo

<video src="https://github.com/user-attachments/assets/8e06d645-d89e-4eb5-8a59-c427afc19c0b" controls width="100%"></video>


<img width="1379" height="752" alt="rtaprojectimage" src="https://github.com/user-attachments/assets/b2e95e7a-f475-492a-89ad-028d86d35622" />


## Project Overview


This repository provides a **full-stack, production-ready solution** for analyzing Telangana RTA Vehicle Registration data. It demonstrates modern data engineering practices, CI/CD automation, and scalable analytics architecture using **Databricks**, **DLT pipelines**, and **Terraform**.

The project is designed for **end-to-end development, testing, deployment, and dashboarding**, following real-world industry standards.

**Key Goals:**

* Ingest, transform, and analyze vehicle registration datasets
* Build modular, production-ready ETL/DLT pipelines
* Ensure **code quality and reliability** through automated testing with `pytest`
* Implement **CI/CD pipelines** using GitHub Actions
* Showcase **Infrastructure-as-Code (IaC)** using Databricks Asset Bundles and Terraform
* Maintain **data governance and environment separation** for DEV and PROD

---

https://github.com/user-attachments/assets/8e06d645-d89e-4eb5-8a59-c427afc19c0b



## Development Workflow
* **Local Setup:**
    * Clone the repo, install Python dependencies via `requirements.txt`.
    * Use development configs in `trvrda/databricks.yml` & `.databricks/bundle/dev/`.
* **Coding Standards:**
    * Follow modular folder and file structure; leverage repo sub-README.md files for context.
    * Employ clear docstrings, type hints, and maintainable code.
* **Typical Tasks:**
    * Build ETL pipelines, run exploratory scripts, update configuration files, and contribute to transformations.

## CI/CD with GitHub Actions
* Automated builds and tests are defined in `.github/workflows/ci.yml`.
* On each push/pull request:
    * Python tests run via pytest
    * Linting and static checks
    * Future expansions: deployment triggers, artifact publishing.

## Testing: Pytest
* Tests are in the `tests/` directory.
* Run tests locally:
    * Install test dependencies (`pip install -r requirements.txt`)
    * Execute `pytest` in the project root
* Testing is automated in CI/CD pipelines.

## Infrastructure as Code (IaC): Asset Bundles & Terraform
* Databricks environment and jobs are provisioned using Asset Bundles (`trvrda/.databricks/bundle/dev/`) and Terraform files.
* Update bundle configs for new clusters or data sources.
* Run Terraform locally for state management and reproducible deployments.

## Data Engineering Lifecycle
* Modular ETL code is under `trvrda/src/trvrda_etl/` (transformations & explorations).
* Adopts Bronze-Silver-Gold architecture for structured data processing.
* Use job templates (e.g., `trvrda_etl.pipeline.yml`, `sample_job.job.yml`) for reusable executions.

## Real-World Best Practices
* Version control for code, configs, and data assets
* End-to-end automation: local-dev → CI/CD → IaC → production
* Code quality, reproducibility, and modularity enable maintainable, scalable workflows
* Data governance considerations in pipeline design & artifacts

## Usage Instructions
1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Review and update configs in `trvrda/databricks.yml` and `.databricks/bundle/dev/`
4. Run `pytest` to validate code
5. Use Databricks CLI / Terraform to provision environments
6. Launch and monitor ETL pipelines and notebook explorations

## Troubleshooting & FAQ
* For common errors, check logs in CI/CD pipeline and Databricks job output
* Update or reinstall local Python environment if dependency conflicts arise
* See submodule README.md files for advanced guides and modular documentation

---