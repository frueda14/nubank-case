import pandas as pd
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, sum as ssum, datediff

def model(dbt, session):
    """
    Snowpark Python model to calculate Customer Lifetime Value (LTV) for Nubank accounts.
    - Reads the int_transactions_union model (PIX and Transfers).
    - Aggregates total spend per account.
    - Computes months active from first to last transaction.
    - Calculates LTV as total_spend / months_active.
    """
    # 1) Load the unified transactions model
    tx_df = session.table(f"{dbt.config.target.schema}.int_transactions_union")

    # 2) Compute metrics per account
    metrics_df = (
        tx_df
        .group_by("account_id")
        .agg(
            ssum(col("signed_amount")).alias("total_spend"),
            datediff(
                "day",
                datediff("day", col("txn_ts"), col("txn_ts")),  # placeholder, will override
                col("txn_ts")
            )
        )
    )
    # Correction: first and last txn dates
    metrics_df = (
        tx_df
        .group_by("account_id")
        .agg(
            ssum(col("signed_amount")).alias("total_spend"),
            datediff("day",
                     col("txn_ts").min(),
                     col("txn_ts").max()
            ).alias("days_active")
        )
        .with_column("months_active", col("days_active") / 30)
        .with_column("ltv", col("total_spend") / col("months_active"))
        .select(
            col("account_id"),
            col("total_spend"),
            col("months_active"),
            col("ltv")
        )
    )

    # 3) Return the Snowpark DataFrame
    return metrics_df
