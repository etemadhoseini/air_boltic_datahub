# Continuous Delivery (CD) for our dbt + Databricks stack  
*A concise, practical plant*

---

## What we’re aiming for
After every merge to `main`, changes move to **Staging**, get **smoke-tested**, then—after a quick **approval**—roll into **Prod**. Deploys are **fast, reversible, and boring**.

---

## The CD flow (end-to-end)

1) **Trigger**  
   - Event: **push/merge to `main`**.  
   - Action: GitHub kicks off a **Databricks Workflow** (a job) that pulls the repo at that commit.

2) **Build to Staging (integration env)**  
   - Run our shared notebook `ops/notebooks/run_dbt` with:  
     - `cmd=build`  
     - `target=stg`  
     - `select=tag:deploy` *(only the surfaces users consume)*  
     - `exclude=tag:heavy` *(optional: skip very expensive models)*  
     - `defer=false` *(staging builds everything it owns)*  
   - Publish `manifest.json` & `run_results.json` to a **stable path** in a UC Volume, e.g.:  
     `/Volumes/ops/dbt_meta/artifacts/stg/<timestamp>/` and also `/stg/latest/`.

3) **Staging smoke tests (automatic)**  
   - Run **schema tests** (contracts), a small set of **business assertions** (e.g., “totals non-negative”, “row count within ±X% of prod”), and **freshness** on key sources.  


4) **Human approval (fast)**  
   - If smoke tests pass, an **approval task** in the Databricks Workflow asks a data owner to promote.  
   - Keep the bar simple: “metrics look sane, contracts green, dashboards not broken”.

5) **Build to Production (deferred to staging)**  
   - Run the same notebook with:  
     - `cmd=build`  
     - `target=prod`  
     - `select=tag:deploy`  
     - `defer=true` + `state_path=/Volumes/ops/dbt_meta/artifacts/stg/latest/`  
       *(so upstream refs resolve to staging’s versions—safe & fast)*  
   - Post-deploy: run a **tiny smoke suite** again (contracts + one guardrail per domain).

6) **Docs & lineage refresh (automatic)**  
   - Publish dbt docs & the **manifest** for discovery/lineage tooling.  
   - Store artifacts per run for auditability and quick diffs.

7) **Rollback plan (clear & quick)**  
   - **Code rollback:** re-run the Workflow at a previous commit.  
   - **Data rollback:** Delta Time Travel on affected prod tables (or rebuild at last good commit with `--full-refresh` where needed).

---

## The pieces we use

- **Databricks Workflows** job with three tasks:
  1. `stg_build` (notebook `run_dbt`)  
  2. `approval_gate` (manual)  
  3. `prod_build` (notebook `run_dbt`, **defer to staging manifest**)  
- **dbt tags** to control scope:  
  - `tag:deploy` = user-facing tables/views we actually ship  
  - `tag:heavy` = skip in staging if you want quicker cycles (can still run nightly)  
- **Artifacts in a UC Volume** (versioned + `latest/` pointer):  
  - `/Volumes/ops/dbt_meta/artifacts/<env>/<timestamp>/manifest.json`  
  - `/Volumes/ops/dbt_meta/artifacts/<env>/latest/manifest.json`

---

## What we test in CD (keep it lean)

- **Contracts**: unique keys, not null, relationships (these are cheap and catch a lot).  
- **Business smoke**: a few domain checks (e.g., revenue ≥ 0, active users not dropping by >20%).
- **Freshness** (staging & prod key sources).

> CI already runs a broader test suite; CD stays light so deploys are fast.


---

## Minimal viable CD (what to implement first)

1. **Databricks Workflow** with `stg_build → approval → prod_build`.  
2. **Deferral** from prod to **staging’s manifest** (`…/stg/latest/manifest.json`).  
3. **tag:deploy** to limit what ships; a **small smoke suite** in staging & prod.  
4. **Artifact publishing** to a UC Volume (per-run + `latest/`).  
5. **Rollback playbook** documented (code + Delta time travel).

> This gives safe, quick releases without over-engineering.
