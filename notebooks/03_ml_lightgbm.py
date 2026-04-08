# Databricks notebook source
# MAGIC %md
# MAGIC # Modelo 2 — LightGBM Regressor
# MAGIC Treina LightGBM com SHAP values para explicabilidade.
# MAGIC Leaf-wise growth e suporte nativo a features categóricas.

# COMMAND ----------

import mlflow
import mlflow.lightgbm
import numpy as np
import pandas as pd
import lightgbm as lgb
import shap
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

CATALOG    = "cartola_fc"
EXPERIMENT = "/Shared/cartola_fc/lightgbm"
MODEL_NAME = "cartola_lightgbm"

mlflow.set_experiment(EXPERIMENT)

# COMMAND ----------

df = spark.table(f"{CATALOG}.gold.feature_store_previsao") \
    .filter("pontuacao_real IS NOT NULL") \
    .toPandas()

print(f"Registros de treino: {len(df):,}")

# COMMAND ----------

FEATURES_NUM = [
    "media_pontos_geral", "media_ultimas_5", "media_ultimas_3",
    "desvio_padrao_pontos", "consistencia_score", "total_jogos",
    "aproveitamento_campo", "preco_atual", "variacao_preco",
    "clube_posicao_tabela", "clube_aproveitamento",
    "adversario_posicao_tabela",
]
FEATURES_CAT = ["posicao_id", "mando_campo", "eh_classico"]

df["mando_campo"]  = df["mando_campo"].astype("category")
df["eh_classico"]  = df["eh_classico"].astype("category")
df["posicao_id"]   = df["posicao_id"].astype("category")

FEATURES = FEATURES_NUM + FEATURES_CAT
TARGET   = "pontuacao_real"
X = df[FEATURES].fillna(0)
y = df[TARGET]

THRESHOLD_MAP = {1: 2.0, 2: 2.5, 3: 2.5, 4: 3.0, 5: 3.5, 6: 2.0}

def coverage_score(y_true, y_pred, posicao_ids, threshold_map):
    thresholds = posicao_ids.map(threshold_map).fillna(2.5)
    return (np.abs(y_true - y_pred) <= thresholds).mean()

# COMMAND ----------
# MAGIC %md ## Treino com TimeSeriesSplit

tscv = TimeSeriesSplit(n_splits=5)
best_score = float("inf")
best_params = {}

param_grid = [
    {"num_leaves": 31,  "learning_rate": 0.05, "min_child_samples": 20, "subsample": 0.8},
    {"num_leaves": 63,  "learning_rate": 0.03, "min_child_samples": 30, "subsample": 0.7},
    {"num_leaves": 127, "learning_rate": 0.01, "min_child_samples": 50, "subsample": 0.9},
]

for params in param_grid:
    cv_scores = []
    for train_idx, val_idx in tscv.split(X):
        X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]

        dtrain = lgb.Dataset(X_train, label=y_train, categorical_feature=FEATURES_CAT)
        dval   = lgb.Dataset(X_val,   label=y_val,   categorical_feature=FEATURES_CAT, reference=dtrain)

        full_params = {**params, "objective": "regression", "metric": "rmse",
                       "verbosity": -1, "n_jobs": -1}
        cb = lgb.train(full_params, dtrain, num_boost_round=500,
                       valid_sets=[dval], callbacks=[lgb.early_stopping(30), lgb.log_evaluation(-1)])

        y_pred = cb.predict(X_val)
        cv_scores.append(np.sqrt(mean_squared_error(y_val, y_pred)))

    score = np.mean(cv_scores)
    if score < best_score:
        best_score = score
        best_params = params

print(f"Melhores params: {best_params} | CV RMSE: {best_score:.3f}")

# COMMAND ----------
# MAGIC %md ## Treino final + SHAP + MLflow

with mlflow.start_run(run_name="lightgbm_final") as run:
    full_params = {**best_params, "objective": "regression", "metric": "rmse",
                   "verbosity": -1, "n_jobs": -1}
    dtrain = lgb.Dataset(X, label=y, categorical_feature=FEATURES_CAT)
    model  = lgb.train(full_params, dtrain, num_boost_round=500)

    y_pred  = model.predict(X)
    rmse    = np.sqrt(mean_squared_error(y, y_pred))
    mae     = mean_absolute_error(y, y_pred)
    r2      = r2_score(y, y_pred)
    cov     = coverage_score(y, y_pred, df["posicao_id"].cat.codes, THRESHOLD_MAP)

    # SHAP values para explicabilidade
    explainer   = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X)
    feature_importance = pd.DataFrame({
        "feature":    FEATURES,
        "shap_mean":  np.abs(shap_values).mean(axis=0)
    }).sort_values("shap_mean", ascending=False)

    mlflow.log_params(best_params)
    mlflow.log_metrics({"rmse": rmse, "mae": mae, "r2_score": r2, "coverage_score": cov})
    mlflow.lightgbm.log_model(model, "model", registered_model_name=MODEL_NAME)

    # Loga importância de features como artefato
    fi_path = "/tmp/feature_importance_lgbm.csv"
    feature_importance.to_csv(fi_path, index=False)
    mlflow.log_artifact(fi_path)

    print(f"RMSE: {rmse:.3f} | MAE: {mae:.3f} | R²: {r2:.3f} | Coverage: {cov:.3f}")
    print("\nTop 5 features (SHAP):")
    print(feature_importance.head(5).to_string(index=False))

    # Previsões para a próxima rodada
    df_prox = spark.table(f"{CATALOG}.gold.feature_store_previsao") \
        .filter("pontuacao_real IS NULL") \
        .toPandas()

    if len(df_prox) > 0:
        df_prox["mando_campo"] = df_prox["mando_campo"].astype("category")
        df_prox["eh_classico"] = df_prox["eh_classico"].astype("category")
        df_prox["posicao_id"]  = df_prox["posicao_id"].astype("category")

        X_prox   = df_prox[FEATURES].fillna(0)
        previsoes = model.predict(X_prox)

        resultado = pd.DataFrame({
            "atleta_id":          df_prox["atleta_id"],
            "rodada_alvo":        df_prox["rodada_alvo"],
            "pontuacao_prevista": previsoes.clip(0),
            "rmse":               rmse,
            "mae":                mae,
            "r2_score":           r2,
            "coverage_score":     cov,
            "modelo_versao":      MODEL_NAME,
            "run_id_mlflow":      run.info.run_id,
            "_generated_at":      pd.Timestamp.utcnow()
        })

        spark.createDataFrame(resultado).write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"{CATALOG}.models.raw_previsoes_lightgbm")

        print(f"Previsões LightGBM salvas: {len(resultado)} atletas")
