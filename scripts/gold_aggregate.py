import pandas as pd
from pathlib import Path


def correr_aggregate_gold(**context):
    silver_file = context["ti"].xcom_pull(key="silver_file")

    df = pd.read_csv(silver_file)

    agg = (
        df.groupby("origin_country")
        .agg(
            total_vuelos=pd.NamedAgg(column="icao24", aggfunc="count"),
            velocidad_media=pd.NamedAgg(column="velocity", aggfunc="mean"),
            altitud_media=pd.NamedAgg(column="geo_altitude", aggfunc="mean")
        )
        .reset_index()
    )

    gold_path = Path(silver_file.replace('silver', 'gold'))
    context["ti"].xcom_push(key="gold_file", value=str(gold_path))
    agg.to_csv(gold_path, index=False)

