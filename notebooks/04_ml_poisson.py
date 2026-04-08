# Databricks notebook source
# MAGIC %md
# MAGIC # Modelo 3 — Poisson-Bayesiano com Regularização
# MAGIC Modela pontuação como contagem/intensidade com priors informativos por posição.
# MAGIC Produz intervalo de credibilidade 80% além do ponto central.

# COMMAND ----------

import mlflow
import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy import stats

CATALOG    = "cartola_fc"
EXPERIMENT = "/Shared/cartola_fc/poisson"
MODEL_NAME = "cartola_poisson"

mlflow.set_experiment(EXPERIMENT)

# COMMAND ----------

df = spark.table(f"{CATALOG}.gold.feature_store_previsao") \
    .filter("pontuacao_real IS NOT NULL") \
    .toPandas()

print(f"Registros de treino: {len(df):,}")

# COMMAND ----------
# MAGIC %md ## Priors informativos por posição
# MAGIC Usamos a média histórica de cada posição como prior bayesiano.

priors_por_posicao = df.groupby("posicao_id")["pontuacao_real"].agg(["mean", "std"]).reset_index()
priors_por_posicao.columns = ["posicao_id", "prior_media", "prior_std"]
df = df.merge(priors_por_posicao, on="posicao_id", how="left")

print("Priors por posição:")
print(priors_por_posicao.to_string(index=False))

# COMMAND ----------
# MAGIC %md ## Features e target

FEATURES = [
    "media_pontos_geral", "media_ultimas_5", "media_ultimas_3",
    "desvio_padrao_pontos", "total_jogos", "aproveitamento_campo",
    "clube_posicao_tabela", "adversario_posicao_tabela",
    "prior_media",
]

# Dummy encode
df["mando_casa"] = (df["mando_campo"] == "casa").astype(int)
df["classico"]   = df["eh_classico"].astype(int)
FEATURES += ["mando_casa", "classico"]

# Adiciona dummies de posição
posicao_dummies = pd.get_dummies(df["posicao_id"], prefix="pos", drop_first=True)
df = pd.concat([df, posicao_dummies], axis=1)
FEATURES += [c for c in posicao_dummies.columns]

X = df[FEATURES].fillna(0).astype(float)
y = df["pontuacao_real"].clip(0)  # Poisson requer y >= 0

X_sm = sm.add_constant(X)

# COMMAND ----------
# MAGIC %md ## Treino GLM Poisson com regularização

THRESHOLD_MAP = {1: 2.0, 2: 2.5, 3: 2.5, 4: 3.0, 5: 3.5, 6: 2.0}

def coverage_score_intervalo(y_true, lower, upper):
    """Cobertura do intervalo de credibilidade 80%."""
    return ((y_true >= lower) & (y_true <= upper)).mean()

with mlflow.start_run(run_name="poisson_final") as run:
    # GLM Poisson
    model = sm.GLM(y, X_sm, family=sm.families.Poisson())
    result = model.fit_regularized(method="elastic_net", alpha=0.1, L1_wt=0.5)

    y_pred = result.predict(X_sm).clip(0)

    # Intervalo de credibilidade 80% via distribuição de Poisson
    lower = pd.Series([stats.poisson.ppf(0.10, max(mu, 0.01)) for mu in y_pred])
    upper = pd.Series([stats.poisson.ppf(0.90, max(mu, 0.01)) for mu in y_pred])

    cov_intervalo = coverage_score_intervalo(y, lower, upper)
    cov_ponto = ((np.abs(y - y_pred) <= df["posicao_id"].map(THRESHOLD_MAP).fillna(2.5))).mean()

    mae     = np.abs(y - y_pred).mean()
    deviance = result.deviance
    llf      = result.llf

    mlflow.log_params({
        "family":   "Poisson",
        "method":   "elastic_net",
        "alpha":    0.1,
        "L1_wt":    0.5,
        "n_features": len(FEATURES)
    })
    mlflow.log_metrics({
        "mae":                mae,
        "deviance":           deviance,
        "log_likelihood":     llf,
        "coverage_score":     cov_ponto,
        "coverage_intervalo": cov_intervalo
    })

    print(f"MAE: {mae:.3f} | Deviance: {deviance:.1f} | Coverage ponto: {cov_ponto:.3f} | Coverage 80% CI: {cov_intervalo:.3f}")

    # COMMAND ----------
    # Previsões para a próxima rodada

    df_prox = spark.table(f"{CATALOG}.gold.feature_store_previsao") \
        .filter("pontuacao_real IS NULL") \
        .toPandas()

    if len(df_prox) > 0:
        df_prox = df_prox.merge(priors_por_posicao, on="posicao_id", how="left")
        df_prox["mando_casa"] = (df_prox["mando_campo"] == "casa").astype(int)
        df_prox["classico"]   = df_prox["eh_classico"].astype(int)

        pos_dummies_prox = pd.get_dummies(df_prox["posicao_id"], prefix="pos", drop_first=True)
        for col in posicao_dummies.columns:
            if col not in pos_dummies_prox.columns:
                pos_dummies_prox[col] = 0
        df_prox = pd.concat([df_prox, pos_dummies_prox[posicao_dummies.columns]], axis=1)

        X_prox    = df_prox[FEATURES].fillna(0).astype(float)
        X_prox_sm = sm.add_constant(X_prox, has_constant="add")
        mu_prox   = result.predict(X_prox_sm).clip(0)
        lower_p   = [stats.poisson.ppf(0.10, max(m, 0.01)) for m in mu_prox]
        upper_p   = [stats.poisson.ppf(0.90, max(m, 0.01)) for m in mu_prox]

        resultado = pd.DataFrame({
            "atleta_id":                        df_prox["atleta_id"],
            "rodada_alvo":                      df_prox["rodada_alvo"],
            "pontuacao_prevista":               mu_prox.clip(0),
            "intervalo_credibilidade_inferior": lower_p,
            "intervalo_credibilidade_superior": upper_p,
            "coverage_score":                   cov_ponto,
            "modelo_versao":                    MODEL_NAME,
            "run_id_mlflow":                    run.info.run_id,
            "_generated_at":                    pd.Timestamp.utcnow()
        })

        spark.createDataFrame(resultado).write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"{CATALOG}.models.raw_previsoes_poisson")

        print(f"Previsões Poisson salvas: {len(resultado)} atletas")
