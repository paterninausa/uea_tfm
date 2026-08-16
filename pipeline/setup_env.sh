#!/usr/bin/env bash
#
# setup_env.sh — Crea el entorno virtual (venv) del pipeline e instala
# sus dependencias.
#
# Prerrequisito: Java 21 gestionado por SDKMAN (ver .sdkmanrc en la raiz
# del repo). Este script NO instala Java — solo verifica que este disponible.
#
# Uso:
#   bash pipeline/setup_env.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_DIR="$REPO_ROOT/.venv"
REQ_FILE="$SCRIPT_DIR/requirements.txt"

echo "==> Verificando Java (requerido: 17+, recomendado 21 LTS)"
if ! command -v java &> /dev/null; then
  echo "ERROR: no se encontro 'java' en el PATH."
  echo "Instala Java 21 vía SDKMAN: sdk env install && sdk env"
  exit 1
fi
java -version

echo ""
echo "==> Creando entorno virtual en $VENV_DIR"
python3 -m venv "$VENV_DIR"

echo "==> Instalando dependencias desde $REQ_FILE"
source "$VENV_DIR/bin/activate"
pip install --upgrade pip
pip install -r "$REQ_FILE"

echo ""
echo "==> Entorno creado correctamente."
echo ""
echo "Para empezar a usarlo:"
echo "  source .venv/bin/activate"
echo ""
echo "Verificacion rapida recomendada tras activar:"
echo "  python -c \"import pyspark; print('PySpark', pyspark.__version__, 'OK')\""
