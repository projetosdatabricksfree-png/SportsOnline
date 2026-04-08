#!/usr/bin/env python3
"""
Script para verificar a estrutura do projeto após reorganização.
Execute: python3 verify-structure.py
"""

import os
from pathlib import Path
from datetime import datetime

def check_structure():
    """Verifica se a estrutura está correta."""
    base = Path("/home/diego/Desktop/DATABRICKS_SPORTS")

    print("=" * 70)
    print("✅ VERIFICAÇÃO DA ESTRUTURA - DATABRICKS SPORTS")
    print("=" * 70)
    print()

    # Verificações
    checks = {
        "projects/databricks-sports": base / "projects" / "databricks-sports",
        "projects/databricks-sports/dbt_project.yml": base / "projects" / "databricks-sports" / "dbt_project.yml",
        "projects/databricks-sports/models": base / "projects" / "databricks-sports" / "models",
        "projects/databricks-sports/tests": base / "projects" / "databricks-sports" / "tests",
        "projects/databricks-sports/logs": base / "projects" / "databricks-sports" / "logs",
        ".agents/skills": base / ".agents" / "skills",
        "skills/skills-lock.json": base / "skills" / "skills-lock.json",
        "venv": base / "venv",
    }

    print("📊 VERIFICAÇÕES:\n")
    all_good = True
    for name, path in checks.items():
        exists = path.exists()
        status = "✅" if exists else "❌"
        print(f"{status} {name}")
        if not exists:
            all_good = False

    print()

    # Mostrar estrutura de databricks-sports
    dbt_path = base / "projects" / "databricks-sports"
    if dbt_path.exists():
        print("📁 CONTEÚDO DE projects/databricks-sports/:\n")
        for item in sorted(dbt_path.iterdir()):
            if not item.name.startswith('.'):
                icon = "📁" if item.is_dir() else "📄"
                print(f"   {icon} {item.name}")

    print()

    # Mostrar skills disponíveis
    skills_path = base / ".agents" / "skills"
    if skills_path.exists():
        print("📚 SKILLS DISPONÍVEIS:\n")
        skills = [d for d in sorted(skills_path.iterdir()) if d.is_dir()]
        for skill in skills:
            print(f"   ✓ {skill.name}")

    print()
    print("=" * 70)
    if all_good:
        print("✅ ESTRUTURA VERIFICADA COM SUCESSO!")
    else:
        print("⚠️  ALGUNS ARQUIVOS ESTÃO FALTANDO - VERIFIQUE!")
    print("=" * 70)

    return all_good

if __name__ == "__main__":
    check_structure()
