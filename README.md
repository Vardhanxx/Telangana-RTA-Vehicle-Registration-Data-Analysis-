# Telangana RTA Vehicle Registration Data Analysis

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

## Architecture & Environment Setup

The solution is built with a **DEV-PROD schema separation** and **view-driven dashboards** for maximum reliability:

### 1. Environment Separation

| Environment | Schema        | Purpose                                              |
| ----------- | ------------- | ---------------------------------------------------- |
| DEV         | `trvrda.dev`  | Sample/lower-volume data for iterative testing       |
| PROD        | `trvrda.prod` | Full production dataset for dashboards and analytics |

### 2. Views for Stable Dashboards

* Views are created in both DEV and PROD schemas with **identical names and column structures**.
* Example:

  * DEV: `trvrda.dev.fact_fuzzy_registrations`
  * PROD: `trvrda.prod.fact_fuzzy_registrations`
* Dashboard queries **always reference views**, not raw tables, ensuring schema-agnostic logic.

### 3. ETL & DLT Pipeline

* **Bronze-Silver-Gold architecture** for structured, incremental data processing.
* **DLT pipelines** process raw data, perform transformations, and populate tables for dashboard consumption.
* Pipelines are **parameterized** to switch between DEV and PROD schemas.

### 4. Dashboard Architecture

* Dashboards consume **DEV or PROD views**.
* Widget logic remains **identical across environments**, simplifying promotion.
* Example: `SELECT * FROM trvrda.dev.fact_fuzzy_registrations` in DEV and `SELECT * FROM trvrda.prod.fact_fuzzy_registrations` in PROD.

---

## Workflow: End-to-End

**Development in DEV**

* Use **sample datasets** in `trvrda.dev` schema
* Create and validate views
* Run exploratory notebooks and transformations

**Code Testing**

* **Pytest** validates notebooks and Python modules
* Tests are run **externally**, not inside DLT pipelines
* CI/CD integration ensures pipelines only trigger if tests pass

**CI/CD Automation**

* GitHub Actions runs on each push/pull request:

  * Linting & static checks
  * `pytest` execution
  * Optional: artifact packaging and deployment triggers

**Deployment**

* Use **Databricks CLI / dbx** to deploy asset bundles and pipelines
* DEV first: run pipeline → verify tables → refresh views → validate dashboard
* PROD next: promote bundle → run full pipeline → attach dashboard to PROD views

**Monitoring & Reliability**

* Automated job triggers for both **view creation** and **data pipelines**
* Environment-specific configurations prevent accidental PROD data corruption
* Dashboards remain **stable and reproducible**

---

## Infrastructure as Code (IaC)

* Databricks clusters, jobs, and pipelines are provisioned using **Asset Bundles**
* Terraform manages state and reproducible deployments
* Supports **multi-environment deployments** with DEV/PROD configurations

---

## Project Structure

```
trvrda/
├─ src/trvrda_etl/        # Modular ETL and DLT pipelines
├─ tests/                 # Pytest test cases for transformations
├─ .databricks/bundle/    # Asset bundles for DEV/PROD
├─ trvrda/databricks.yml  # Environment configs
├─ trvrda_etl.pipeline.yml # Pipeline definition
├─ sample_job.job.yml      # Job template
├─ requirements.txt       # Python dependencies
└─ .github/workflows/ci.yml # CI/CD automation
```

---

## Testing

* Tests ensure **data transformation correctness**, schema compliance, and business logic integrity
* Run locally:

```bash
pip install -r requirements.txt
pytest
```

* CI/CD pipeline automates testing before deployment

---

## Industry Best Practices Applied

* **DEV-PROD separation** for safe, incremental deployments
* **View-based dashboards** for schema stability
* **Automated CI/CD with testing** ensures reliability
* **IaC with Terraform & Asset Bundles** for reproducibility
* **Modular ETL codebase** for maintainability
* **Bronze-Silver-Gold data architecture** for incremental processing

---

## Quick Start

1. Clone repository:

```bash
git clone <repo_url>
cd trvrda
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Update environment configs in `trvrda/databricks.yml`
4. Run tests:

```bash
pytest
```

5. Provision DEV cluster & jobs with Databricks CLI / Terraform
6. Run ETL/DLT pipelines → verify DEV dashboard
7. Promote to PROD → attach dashboard to PROD views

---

## Use Cases

* Vehicle registration trends and insights for Telangana RTA
* Analysis of registration anomalies or fuzzy matches
* Automated dashboards for **policy decision-making**
* Real-world template for **enterprise data engineering** with production-grade pipelines

---

This README now **consolidates architecture, workflow, testing, CI/CD, and dashboards**, reflecting **real-world industry practices** for end-to-end data pipelines.
