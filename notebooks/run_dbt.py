# Databricks notebook source
# MAGIC %md
# MAGIC # Runner dbt — Executa comandos dbt no Databricks
# MAGIC Recebe o parâmetro `dbt_command` e executa no contexto do projeto.

# COMMAND ----------

import subprocess
import sys

# COMMAND ----------

# Recebe o comando dbt via widget/parâmetro
dbutils.widgets.text("dbt_command", "run", "Comando dbt")
dbt_command = dbutils.widgets.get("dbt_command")

print(f"Executando: dbt {dbt_command}")

# COMMAND ----------

# Instala dbt-databricks se necessário
subprocess.check_call([sys.executable, "-m", "pip", "install", "dbt-databricks", "-q"])

# COMMAND ----------

# Executa cada comando dbt (separados por &&)
commands = [cmd.strip() for cmd in dbt_command.split("&&")]

for cmd in commands:
    full_cmd = cmd if cmd.startswith("dbt ") else f"dbt {cmd}"
    print(f"\n{'='*60}")
    print(f"Executando: {full_cmd}")
    print(f"{'='*60}\n")

    result = subprocess.run(
        full_cmd.split(),
        capture_output=True,
        text=True,
        cwd="/Workspace/Users/{}/{}".format(
            spark.conf.get("spark.databricks.notebook.path", "").split("/")[3] if "/" in spark.conf.get("spark.databricks.notebook.path", "") else "",
            "cartola_fc_pipeline"
        )
    )

    print(result.stdout)
    if result.stderr:
        print(f"STDERR: {result.stderr}")

    if result.returncode != 0:
        raise Exception(f"dbt falhou com código {result.returncode}: {result.stderr}")

print("\nTodos os comandos dbt executados com sucesso!")
