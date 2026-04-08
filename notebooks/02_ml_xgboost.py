# Databricks notebook source
# MAGIC %md
# MAGIC # Modelo 1 — XGBoost Regressor
# MAGIC Treina XGBoost com Hyperopt para prever pontuação de atletas na próxima rodada.
# MAGIC Rastreia experimento e registra modelo no MLflow.

# COMMAND ----------

import mlflow
import mlflow.xgboost
import numpy as np
import pandas as pd
import xgboost as xgb
from hyperopt import fmin, tpe, hp, STATUS_OK, Trials
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

CATALOG        = "cartola_fc"
EXPERIMENT     = "/Shared/cartola_fc/xgboost"
MODEL_NAME     = "cartola_xgboost"

mlflow.set_experiment(EXPERIMENT)

# COMMAND ----------
# MAGIC %md ## 1. Carga de dados

df = spark.table(f"{CATALOG}.gold.feature_store_previsao") \
    .filter("pontuacao_real IS NOT NULL") \
    .toPandas()

print(f"Registros de treino: {len(df):,}")
print(f"Rodadas: {df['rodada_alvo'].min()} → {df['rodada_alvo'].max()}")

# COMMAND ----------
# MAGIC %md ## 2. Feature engineering

FEATURES = [
    "media_pontos_geral", "media_ultimas_5", "media_ultimas_3",
    "desvio_padrao_pontos", "consistencia_score", "total_jogos",
    "aproveitamento_campo", "preco_atual", "variacao_preco",
    "posicao_id", "clube_posicao_tabela", "clube_aproveitamento",
    "adversario_posicao_tabela",
]

TARGET = "pontuacao_real"

# Codifica variáveis categóricas
df["mando_campo_enc"] = (df["mando_campo"] == "casa").astype(int)
df["eh_classico_enc"] = df["eh_classico"].astype(int)
FEATURES += ["mando_campo_enc", "eh_classico_enc"]

X = df[FEATURES].fillna(0)
y = df[TARGET]
rodadas = df["rodada_alvo"]

# COMMAND ----------
# MAGIC %md ## 3. Validação com TimeSeriesSplit

tscv = TimeSeriesSplit(n_splits=5)

def coverage_score(y_true, y_pred, posicao_ids, threshold_map):
    """Calcula cobertura: % de previsões dentro do threshold por posição."""
    thresholds = posicao_ids.map(threshold_map).fillna(2.5)
    within = np.abs(y_true - y_pred) <= thresholds
    return within.mean()

THRESHOLD_MAP = {1: 2.0, 2: 2.5, 3: 2.5, 4: 3.0, 5: 3.5, 6: 2.0}

# COMMAND ----------
# MAGIC %md ## 4. Otimização com Hyperopt

def objective(params):
    params["max_depth"]        = int(params["max_depth"])
    params["min_child_weight"] = int(params["min_child_weight"])

    rmse_scores = []
    for train_idx, val_idx in tscv.split(X):
        X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]

        model = xgb.XGBRegressor(**params, n_estimators=500, early_stopping_rounds=30,
                                  random_state=42, n_jobs=-1)
        model.fit(X_train, y_train,
                  eval_set=[(X_val, y_val)],
                  verbose=False)

        y_pred = model.predict(X_val)
        rmse_scores.append(np.sqrt(mean_squared_error(y_val, y_pred)))

    return {"loss": np.mean(rmse_scores), "status": STATUS_OK}

search_space = {
    "learning_rate":    hp.loguniform("learning_rate", np.log(0.01), np.log(0.3)),
    "max_depth":        hp.quniform("max_depth", 3, 10, 1),
    "min_child_weight": hp.quniform("min_child_weight", 1, 10, 1),
    "subsample":        hp.uniform("subsample", 0.6, 1.0),
    "colsample_bytree": hp.uniform("colsample_bytree", 0.6, 1.0),
    "reg_alpha":        hp.loguniform("reg_alpha", np.log(1e-4), np.log(10)),
    "reg_lambda":       hp.loguniform("reg_lambda", np.log(1e-4), np.log(10)),
}

trials = Trials()
best_params = fmin(fn=objective, space=search_space, algo=tpe.suggest,
                   max_evals=50, trials=trials)
best_params["max_depth"]        = int(best_params["max_depth"])
best_params["min_child_weight"] = int(best_params["min_child_weight"])

print(f"Melhores hiperparâmetros: {best_params}")

# COMMAND ----------
# MAGIC %md ## 5. Treino final e registro no MLflow

with mlflow.start_run(run_name="xgboost_final") as run:
    # Treino no conjunto completo
    model = xgb.XGBRegressor(**best_params, n_estimators=500, random_state=42, n_jobs=-1)
    model.fit(X, y)

    # Métricas no conjunto completo (validação via CV já foi feita)
    y_pred_all = model.predict(X)
    rmse  = np.sqrt(mean_squared_error(y, y_pred_all))
    mae   = mean_absolute_error(y, y_pred_all)
    r2    = r2_score(y, y_pred_all)
    cov   = coverage_score(y, y_pred_all, df["posicao_id"], THRESHOLD_MAP)

    mlflow.log_params(best_params)
    mlflow.log_metrics({"rmse": rmse, "mae": mae, "r2_score": r2, "coverage_score": cov})
    mlflow.xgboost.log_model(model, "model", registered_model_name=MODEL_NAME)

    print(f"RMSE: {rmse:.3f} | MAE: {mae:.3f} | R²: {r2:.3f} | Coverage: {cov:.3f}")

    # COMMAND ----------
    # MAGIC %md ## 6. Gera previsões para a próxima rodada

    df_prox = spark.table(f"{CATALOG}.gold.feature_store_previsao") \
        .filter("pontuacao_real IS NULL") \
        .toPandas()

    if len(df_prox) > 0:
        df_prox["mando_campo_enc"] = (df_prox["mando_campo"] == "casa").astype(int)
        df_prox["eh_classico_enc"] = df_prox["eh_classico"].astype(int)

        X_prox = df_prox[FEATURES].fillna(0)
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
            .saveAsTable(f"{CATALOG}.models.raw_previsoes_xgboost")

        print(f"Previsões XGBoost salvas: {len(resultado)} atletas")
