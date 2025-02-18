{% docs __overview__ %}

# Welcome to the Flipside Crypto's Stellar Models Documentation

## **What does this documentation cover?**
The documentation included here details the design of Stellar tables and views available via [Flipside Crypto.](https://flipsidecrypto.xyz/) For more information on how these models are built, please see [the github repository.](https://github.com/flipsideCrypto/stellar-models/)

## **How do I use these docs?**
The easiest way to navigate this documentation is to use the Quick Links below. These links will take you to the documentation for each table, which contains a description, a list of the columns, and other helpful information.

If you are experienced with dbt docs, feel free to use the sidebar to navigate the documentation, as well as explore the relationships between tables and the logic building them.

There is more information on how to use dbt docs in the last section of this document.

## **Quick Links to Table Documentation**

**Click on the links below to jump to the documentation for each schema.**

### Core Tables (`stellar`.`CORE`.`<table_name>`)

**Dimension Tables:**
- [core.dim_assets](https://flipsidecrypto.github.io/stellar-models/#!/model/model.stellar_models.core__dim_assets)
- [core.dim_labels](https://flipsidecrypto.github.io/stellar-models/#!/model/model.stellar_models.core__dim_labels)

**Fact Tables:**
- [core.fact_accounts](https://flipsidecrypto.github.io/stellar-models/#!/model/model.stellar_models.core__fact_accounts)
- [core.fact_ledgers](https://flipsidecrypto.github.io/stellar-models/#!/model/model.stellar_models.core__fact_ledgers)
- [core.fact_operations](https://flipsidecrypto.github.io/stellar-models/#!/model/model.stellar_models.core__fact_operations)
- [core.fact_transactions](https://flipsidecrypto.github.io/stellar-models/#!/model/model.stellar_models.core__fact_transactions)

**Convenience Views:**
- [core.ez_operations](https://flipsidecrypto.github.io/stellar-models/#!/model/model.stellar_models.core__ez_operations)

### DeFi Tables (`stellar`.`DEFI`.`<table_name>`)

**Fact Tables:**
- [defi.fact_liquidity_pools](https://flipsidecrypto.github.io/stellar-models/#!/model/model.stellar_models.defi__fact_liquidity_pools)
- [defi.fact_trades](https://flipsidecrypto.github.io/stellar-models/#!/model/model.stellar_models.defi__fact_trades)

### Price Tables (`stellar`.`PRICE`.`<table_name>`)

**Dimension Tables:**
- [price.dim_asset_metadata](https://flipsidecrypto.github.io/stellar-models/#!/model/model.stellar_models.price__dim_asset_metadata)

**Fact Tables:**
- [price.fact_prices_ohlc_hourly](https://flipsidecrypto.github.io/stellar-models/#!/model/model.stellar_models.price__fact_prices_ohlc_hourly)

**Convenience Views:**
- [price.ez_prices_hourly](https://flipsidecrypto.github.io/stellar-models/#!/model/model.stellar_models.price__ez_prices_hourly)
- [price.ez_asset_metadata](https://flipsidecrypto.github.io/stellar-models/#!/model/model.stellar_models.price__ez_asset_metadata)

### Stats Tables (`stellar`.`STATS`.`<table_name>`)

- [stats.ez_core_metrics_hourly](https://flipsidecrypto.github.io/stellar-models/#!/model/model.stellar_models.stats__ez_core_metrics_hourly)

## **Data Model Overview**

These Stellar models are built a few different ways, but the core fact tables are built using three layers of sql models: **bronze, silver, and gold (or core).**

- Bronze: Data is loaded in from the source as a view
- Silver: All necessary parsing, filtering, de-duping, and other transformations are done here
- Gold (or core): Final views and tables that are available publicly

The dimension tables are sourced from a variety of on-chain and off-chain sources.

Convenience views (denoted ez_) are a combination of different fact and dimension tables. These views are built to make it easier to query the data.

## **Using dbt docs**
### Navigation

You can use the ```Project``` and ```Database``` navigation tabs on the left side of the window to explore the models in the project.

### Database Tab

This view shows relations (tables and views) grouped into database schemas. Note that ephemeral models are *not* shown in this interface, as they do not exist in the database.

### Graph Exploration

You can click the blue icon on the bottom-right corner of the page to view the lineage graph of your models.

On model pages, you'll see the immediate parents and children of the model you're exploring. By clicking the Expand button at the top-right of this lineage pane, you'll be able to see all of the models that are used to build, or are built from, the model you're exploring.

Once expanded, you'll be able to use the ```--models``` and ```--exclude``` model selection syntax to filter the models in the graph. For more information on model selection, check out the [dbt docs](https://docs.getdbt.com/docs/model-selection-syntax).

Note that you can also right-click on models to interactively filter and explore the graph.


### **More information**
- [Flipside](https://flipsidecrypto.xyz/)
- [Velocity](https://app.flipsidecrypto.com/velocity?nav=Discover)
- [Tutorials](https://docs.flipsidecrypto.com/our-data/tutorials)
- [Github](https://github.com/FlipsideCrypto/stellar-models)
- [What is dbt?](https://docs.getdbt.com/docs/introduction)

{% enddocs %}