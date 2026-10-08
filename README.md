# Supply Chain Demo: Medallion Architecture on Microsoft Fabric

End-to-end demo that takes raw **delivery truck trip** data from Bronze → Silver → Gold in
Fabric lakehouses and serves it through a **Direct Lake semantic model** built as a proper
Power BI star schema, with a one-page report on top.

Dataset: [Delivery truck trips data](https://www.kaggle.com/datasets/ramakrishnanthiyagu/delivery-truck-trips-data)
(Kaggle, ~6.9K bookings across India, 2019–2020). A copy is in [`data-source/`](data-source/).

## Demo scope

| In scope | Out of scope |
|---|---|
| Bronze via a OneLake **table shortcut** that turns the xlsx into Delta (no code) | Streaming / incremental CDC |
| Silver cleansing, typing, de-duplication, quarantine table | Row-level security |
| Gold Kimball star schema (1 fact, 10 dimensions) | Multiple environments (dev/test/prod) |
| Direct Lake semantic model with measures and hierarchies | Data quality alerting |
| Orchestration pipeline + one-page Power BI report | |

## Architecture

```mermaid
flowchart LR
    subgraph Fabric workspace: supply-chain-demo
      X[(lh_bronze<br/>Files/raw_data/*.xlsx)] -- table shortcut<br/>with transform --> B[lh_bronze<br/>dbo.raw_data]
      B -- nb_02 --> S[lh_silver<br/>dbo.trips<br/>dbo.trips_rejected]
      S -- nb_03 --> G[lh_gold<br/>fact_trip + dim_*]
      G -- Direct Lake --> M[Semantic model<br/>Supply Chain Trips]
      M --> R[Report<br/>Supply Chain Trips Overview]
    end
    P[[pl_supply_chain_medallion]] -. nb_02 → nb_03 → nb_04 refresh .-> S
```

| Layer | Item | What it does |
|---|---|---|
| Bronze | `dbo.raw_data` table shortcut | Fabric shortcut transformation keeps the xlsx in `Files/raw_data` synced to a Delta table and adds `__filepath__` / `__ingestion_timestamp__` lineage. No notebook. |
| Silver | `nb_02_silver_transform` | `raw_data` → typed, snake_case, one row per `booking_id`. Treats `NULL`/`NA` as nulls, parses `trip_end_date` text, parses `Place, City, State` (PIN codes stripped, missing state inferred from the city), normalises vehicle types, derives delivery status and lead time, and adds DQ flags. Rows it can't use go to `trips_rejected`. Drops PII (driver phone). |
| Gold | `nb_03_gold_star_schema` | Star schema with integer surrogate keys, an `Unknown` (-1) member in every dimension, V-Order, and orphan-key validation. |
| Serve | `nb_04_refresh_semantic_model` | Reframes the Direct Lake model after Gold loads (Semantic Link). |

> **Data-quality finding:** the Excel shortcut transform infers types and can't be configured
> (it only offers *header row* and *sheets*). `trip_start_date`, `actual_eta`, `Planned_ETA` and
> `Data_Ping_time` come through empty, so Silver drops them. **Booking date** is the trip date: in
> the source file, trip start falls on the booking date for 92% of trips. **Lead time** (booking →
> trip end) replaces trip duration.

## Semantic model: *Supply Chain Trips*

```mermaid
erDiagram
    Trips }o--|| Date : "Booking (active) / Trip End (inactive)"
    Trips }o--|| "Origin Location" : ""
    Trips }o--|| "Destination Location" : ""
    Trips }o--|| Vehicle : ""
    Trips }o--|| "Vehicle Type" : ""
    Trips }o--|| Customer : ""
    Trips }o--|| Carrier : ""
    Trips }o--|| Driver : ""
    Trips }o--|| Material : ""
    Trips }o--|| "Trip Details" : ""
```

- **Direct Lake** on `lh_gold`; single-direction one-to-many relationships on integer keys.
- **Date** is a marked date table that plays two roles: booking date (active) and trip end (inactive, used through `USERELATIONSHIP` in *Completed Trips by End Date*).
- **Origin and Destination Location** are role-playing copies of one conformed location dimension. **Trip Details** is a junk dimension (booking type, GPS provider, delivery status).
- Keys and aggregated columns are hidden and implicit measures are disabled. Every visible object has a description. The model has hierarchies for Calendar, Origin and Destination Geography, and Vehicle Classification.
- 18 measures on `Trips`, grouped into display folders:
  - **Volume**: Trips, Completed and Open Trips, Completed Trips by End Date, Previous Month, MoM %, YTD
  - **Delivery Performance**: On-Time %, Delayed %
  - **Distance & Time**: Total and Average Distance, Average Lead Time (days)
  - **Fleet & Partners**: Active Vehicles, Trips per Vehicle, Active Customers, Active Carriers

## Repository layout

```
data-source/                      raw Kaggle files
fabric/notebooks/                 3 PySpark notebooks (.ipynb)
fabric/semantic-model/            TMDL for "Supply Chain Trips" (Direct Lake)
fabric/report/                    PBIR for "Supply Chain Trips Overview"
fabric/pipeline/                  pipeline definition
scripts/deploy.py                 idempotent deploy via Fabric REST API
```

## Run it

Prerequisites: a Fabric capacity workspace, Azure CLI signed in (`az login`), and Python 3.10+.

1. Create a schema-enabled lakehouse `lh_bronze`, then upload `data-source/delivery_truck_trip_data.xlsx` to `Files/raw_data/`. In `Tables/dbo`, create a **table shortcut** named `raw_data` that points at `Files/raw_data` with the Excel → Delta transformation and *first row as headers*.
2. Deploy everything and run the pipeline:
   ```bash
   python scripts/deploy.py --workspace supply-chain-demo --run
   ```
   This creates `lh_silver` and `lh_gold`, plus the notebooks, semantic model, report and pipeline, and resolves every ID by name. Re-running it updates items in place.
3. Open **Supply Chain Trips Overview** in the workspace.

Options: use `--only notebooks semantic-model` to redeploy specific parts, and `--run-notebooks nb_02_silver_transform` to run notebooks directly.

## Expected results

| Check | Value |
|---|---|
| Bronze `dbo.raw_data` rows | 6,880 |
| Silver trips / rejected / duplicates removed | 6,875 / 0 / 5 |
| `fact_trip` rows, orphan keys | 6,875, 0 |
| Model: Trips · On-Time % · Avg Lead Time · Active Vehicles | 6,875 · 36.8% · 6.1 days · 2,312 |
