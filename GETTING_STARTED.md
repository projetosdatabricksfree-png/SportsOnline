# 🚀 Getting Started - Databricks Sports

## O que foi reorganizado?

Sua estrutura de projeto foi melhorada para separar claramente:
- ✅ **Projetos ativos** → pasta `projects/`
- ✅ **Skills Databricks** → pasta `.agents/skills/` (centralizada)
- ✅ **Configurações** → `skills/` e `venv/`

## 📂 Onde estão as coisas agora?

| De | Para | Razão |
|---|---|---|
| `databrickssports/` | `projects/databricks-sports/` | Melhor organização com futuros projetos |
| `logs/` (raiz) | `projects/databricks-sports/logs/` | Mantém logs do projeto junto |
| `.claude/` (raiz) | `projects/databricks-sports/.claude/` | Configuração do projeto |

## 🎯 Próximas Ações

### 1. Verificar a estrutura
```bash
cd /home/diego/Desktop/DATABRICKS_SPORTS
ls -la projects/
ls -la projects/databricks-sports/
```

### 2. Configurar Git (se ainda não tiver)
```bash
cd /home/diego/Desktop/DATABRICKS_SPORTS
git init
git add .
git commit -m "Initial commit: reorganized project structure"
```

### 3. Ativar ambiente Python
```bash
source venv/bin/activate
```

### 4. Trabalhar com dbt
```bash
cd projects/databricks-sports/
dbt deps
dbt run
```

## 📖 Documentação Disponível

- **README.md** → Visão geral do projeto
- **STRUCTURE.md** → Explicação detalhada das pastas
- **TREE.txt** → Visualização da árvore de pastas
- **GETTING_STARTED.md** → Este arquivo

## 🔗 Skills Disponíveis

Acesse via slash commands `/`:

```
/databricks-core                 # CLI, autenticação, exploração
/databricks-pipelines            # Lakeflow Spark DLT
/databricks-jobs                 # Lakeflow Jobs
/databricks-dabs                 # Asset Bundles
/databricks-apps                 # Dashboards e Apps
/databricks-lakebase             # Postgres Autoscaling
/databricks-model-serving        # Model Serving
```

## ❓ Perguntas Frequentes

### P: Posso adicionar mais projetos?
**R:** Sim! Crie em `projects/novo-projeto/`

### P: E as pastas `.goose/`, `.bob/`, etc?
**R:** Eram pastas de agentes de IA. Ainda estão lá se precisar, mas você pode deletar se não usar.

### P: O .gitignore está correto?
**R:** Sim, ignora venv, logs, target do dbt, e pastas de agentes não usados.

### P: Como remover as pastas de agentes não usados?
**R:** Você pode deletar `.goose/`, `.bob/`, `.claude/` (raiz), etc. Mas mantenha `.agents/` (com a referência oficial).

## ✨ Pronto!

Sua estrutura está organizada e pronta para trabalhar. Qualquer dúvida, consulte a documentação nos arquivos .md

---

**Reorganização concluída em:** 2026-04-08  
**Status:** ✅ Pronto para usar
