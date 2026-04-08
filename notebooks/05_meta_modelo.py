# Databricks notebook source
# MAGIC %md
# MAGIC # Meta-Modelo — Coverage Scoring + Previsão Final
# MAGIC Avalia o coverage score de cada modelo base nas últimas 3 rodadas.
# MAGIC Seleciona dinamicamente o melhor modelo (ou ensemble ponderado) por posição.
# MAGIC Gera a previsão final consolidada.

# COMMAND ----------

import mlflow
import numpy as np
import pandas as pd

CATALOG    = "cartola_fc"
EXPERIMENT = "/Shared/cartola_fc/meta_modelo"

mlflow.set_experiment(EXPERIMENT)

# THRESHOLD por posicao_id
THRESHOLD_MAP = {1: 2.0, 2: 2.5, 3: 2.5, 4: 3.0, 5: 3.5, 6: 2.0}
COVERAGE_MIN  = 0.70   # Coverage isolado mínimo para usar só aquele modelo
JANELA        = 3       # Últimas N rodadas para avaliar

# COMMAND ----------
# MAGIC %md ## 1. Carrega previsões históricas dos 3 modelos

xgb = spark.table(f"{CATALOG}.models.raw_previsoes_xgboost").toPandas()
lgb = spark.table(f"{CATALOG}.models.raw_previsoes_lightgbm").toPandas()
poi = spark.table(f"{CATALOG}.models.raw_previsoes_poisson").toPandas()
real = spark.table(f"{CATALOG}.gold.feature_store_previsao") \
    .filter("pontuacao_real IS NOT NULL") \
    .select("atleta_id", "rodada_alvo", "pontuacao_real", "posicao_id") \
    .toPandas()

def merge_com_real(df_modelo, nome):
    return df_modelo.merge(
        real, on=["atleta_id", "rodada_alvo"], how="inner"
    ).assign(modelo=nome)

hist = pd.concat([
    merge_com_real(xgb, "xgboost"),
    merge_com_real(lgb, "lightgbm"),
    merge_com_real(poi, "poisson"),
])

print(f"Registros históricos: {len(hist):,}")

# COMMAND ----------
# MAGIC %md ## 2. Coverage por modelo, posição e rodada (janela deslizante)

rodada_max = hist["rodada_alvo"].max()
hist_janela = hist[hist["rodada_alvo"] >= rodada_max - JANELA + 1].copy()

def calc_coverage(row):
    threshold = THRESHOLD_MAP.get(int(row["posicao_id"]), 2.5)
    return 1.0 if abs(row["pontuacao_prevista"] - row["pontuacao_real"]) <= threshold else 0.0

hist_janela["acerto"] = hist_janela.apply(calc_coverage, axis=1)

coverage_por_posicao = hist_janela.groupby(["modelo", "posicao_id"]).agg(
    coverage=("acerto", "mean"),
    n=("acerto", "count")
).reset_index()

print("\nCoverage por modelo e posição (últimas 3 rodadas):")
print(coverage_por_posicao.pivot(index="posicao_id", columns="modelo", values="coverage").round(3).to_string())

# COMMAND ----------
# MAGIC %md ## 3. Seleção adaptativa por posição

def selecionar_modelo_posicao(posicao_id: int, coverage_df: pd.DataFrame):
    """
    Retorna (modelo_selecionado, pesos_dict).
    Se um modelo tem coverage > COVERAGE_MIN, usa só ele.
    Senão, média ponderada pelo coverage.
    """
    sub = coverage_df[coverage_df["posicao_id"] == posicao_id].set_index("modelo")
    if sub.empty:
        return "ensemble", {"xgboost": 1/3, "lightgbm": 1/3, "poisson": 1/3}

    melhor = sub["coverage"].idxmax()
    if sub.loc[melhor, "coverage"] >= COVERAGE_MIN:
        return melhor, {melhor: 1.0}

    total = sub["coverage"].sum()
    if total == 0:
        pesos = {m: 1/len(sub) for m in sub.index}
    else:
        pesos = (sub["coverage"] / total).to_dict()
    return "ensemble", pesos

selecao = {}
for pos in range(1, 7):
    modelo, pesos = selecionar_modelo_posicao(pos, coverage_por_posicao)
    selecao[pos] = {"modelo": modelo, "pesos": pesos}
    print(f"Posição {pos}: {modelo} | pesos: {pesos}")

# COMMAND ----------
# MAGIC %md ## 4. Gera previsão final para a próxima rodada

xgb_prox = spark.table(f"{CATALOG}.models.raw_previsoes_xgboost") \
    .filter(f"rodada_alvo = (SELECT max(rodada_alvo) FROM {CATALOG}.models.raw_previsoes_xgboost)") \
    .toPandas()
lgb_prox = spark.table(f"{CATALOG}.models.raw_previsoes_lightgbm") \
    .filter(f"rodada_alvo = (SELECT max(rodada_alvo) FROM {CATALOG}.models.raw_previsoes_lightgbm)") \
    .toPandas()
poi_prox = spark.table(f"{CATALOG}.models.raw_previsoes_poisson") \
    .filter(f"rodada_alvo = (SELECT max(rodada_alvo) FROM {CATALOG}.models.raw_previsoes_poisson)") \
    .toPandas()

atletas_info = spark.table(f"{CATALOG}.gold.feature_store_previsao") \
    .filter("pontuacao_real IS NULL") \
    .select("atleta_id", "rodada_alvo", "posicao_id") \
    .toPandas()

previsoes_merged = atletas_info \
    .merge(xgb_prox[["atleta_id", "rodada_alvo", "pontuacao_prevista", "coverage_score", "run_id_mlflow"]].rename(
        columns={"pontuacao_prevista": "prev_xgb", "coverage_score": "cov_xgb", "run_id_mlflow": "run_xgb"}),
        on=["atleta_id", "rodada_alvo"], how="left") \
    .merge(lgb_prox[["atleta_id", "rodada_alvo", "pontuacao_prevista", "coverage_score"]].rename(
        columns={"pontuacao_prevista": "prev_lgb", "coverage_score": "cov_lgb"}),
        on=["atleta_id", "rodada_alvo"], how="left") \
    .merge(poi_prox[["atleta_id", "rodada_alvo", "pontuacao_prevista", "coverage_score"]].rename(
        columns={"pontuacao_prevista": "prev_poi", "coverage_score": "cov_poi"}),
        on=["atleta_id", "rodada_alvo"], how="left")

def calcular_previsao_final(row):
    pos = int(row["posicao_id"]) if not pd.isna(row["posicao_id"]) else 4
    sel = selecao.get(pos, {"modelo": "ensemble", "pesos": {"xgboost": 1/3, "lightgbm": 1/3, "poisson": 1/3}})
    modelo = sel["modelo"]
    pesos  = sel["pesos"]

    prevs = {
        "xgboost":  row.get("prev_xgb", np.nan),
        "lightgbm": row.get("prev_lgb", np.nan),
        "poisson":  row.get("prev_poi", np.nan),
    }

    if modelo != "ensemble":
        val = prevs.get(modelo, np.nan)
        if not pd.isna(val):
            return val, modelo, sel.get("coverage", 0.0)

    # Ensemble ponderado (ignora NaN)
    total_peso = 0.0
    soma = 0.0
    for m, w in pesos.items():
        v = prevs.get(m, np.nan)
        if not pd.isna(v):
            soma += v * w
            total_peso += w

    val_final = soma / total_peso if total_peso > 0 else 0.0
    cov_media = np.nanmean([row.get("cov_xgb", 0), row.get("cov_lgb", 0), row.get("cov_poi", 0)])
    return max(val_final, 0.0), "ensemble", cov_media

result_rows = []
for _, row in previsoes_merged.iterrows():
    prev, modelo_sel, cov = calcular_previsao_final(row)
    result_rows.append({
        "atleta_id":            row["atleta_id"],
        "rodada_alvo":          row["rodada_alvo"],
        "pontuacao_prevista":   prev,
        "modelo_selecionado":   modelo_sel,
        "coverage_score_modelo": cov,
        "_generated_at":        pd.Timestamp.utcnow()
    })

resultado_df = pd.DataFrame(result_rows)

# COMMAND ----------
# MAGIC %md ## 5. Persiste e registra no MLflow

with mlflow.start_run(run_name="meta_modelo") as run:
    mlflow.log_params({
        "janela_rodadas": JANELA,
        "coverage_min":   COVERAGE_MIN,
    })
    for pos, sel in selecao.items():
        mlflow.log_param(f"modelo_pos_{pos}", sel["modelo"])

    spark.createDataFrame(resultado_df).write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{CATALOG}.models.raw_previsao_final")

    print(f"\nPrevisão final salva: {len(resultado_df)} atletas")
    print(f"Distribuição de modelos:\n{resultado_df['modelo_selecionado'].value_counts().to_string()}")

# COMMAND ----------
# MAGIC %md ## 6. Top 5 por posição

top = spark.table(f"{CATALOG}.models.previsao_final") \
    .filter(f"rodada_alvo = {resultado_df['rodada_alvo'].max()}") \
    .orderBy("posicao", "ranking_posicao") \
    .limit(30) \
    .toPandas()

print("\nTop atletas por posição:")
for pos in ["GOL", "LAT", "ZAG", "MEI", "ATA", "TEC"]:
    sub = top[top["posicao"] == pos].head(3)
    if len(sub) > 0:
        print(f"\n{pos}:")
        for _, r in sub.iterrows():
            print(f"  {r['apelido']:20s} | Prev: {r['pontuacao_prevista']:5.1f} | "
                  f"Preço: C${r['preco_atual']:5.2f} | P/C: {r['pontos_por_cartoleta'] or 0:.2f}")
