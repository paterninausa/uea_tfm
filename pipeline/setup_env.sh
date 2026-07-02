#!/usr/bin/env bash
#
# setup_env.sh — Crea el entorno conda "tfm" y corrige un problema conocido
# de PATH con SDKMAN (u otros gestores de JDK) que pisan al JDK instalado
# por conda dentro del propio entorno.
#
# Problema que soluciona:
#   Si tienes SDKMAN (o similar) instalado, su script de inicialización se
#   inyecta en el PATH con más prioridad que el bin/ del entorno conda activo.
#   Resultado: aunque `environment.yml` instala openjdk=11 correctamente
#   dentro de conda (y JAVA_HOME apunta bien a él), el comando `java` suelto
#   resuelve al JDK del sistema (p. ej. Temurin 21) en vez del de conda.
#   PyFlink internamente usa JAVA_HOME, así que probablemente funcione de
#   todos modos, pero deja el entorno inconsistente y confuso para depurar.
#
# Solución:
#   Un script de activación específico del entorno (activate.d) que antepone
#   $CONDA_PREFIX/bin al PATH SOLO mientras el entorno "tfm" está activo.
#   No modifica tu .bashrc/.zshrc global, así que no afecta a SDKMAN ni a
#   otros proyectos que dependan de otras versiones de Java.
#
# Uso:
#   bash pipeline/setup_env.sh
#
set -euo pipefail

ENV_NAME="tfm"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/environment.yml"

echo "==> Creando entorno conda '$ENV_NAME' desde $ENV_FILE"
conda env create -f "$ENV_FILE" || {
  echo "El entorno ya existe. Si quieres recrearlo desde cero:"
  echo "  conda env remove -n $ENV_NAME"
  echo "  bash $0"
  exit 1
}

CONDA_BASE="$(conda info --base)"
ENV_PREFIX="$CONDA_BASE/envs/$ENV_NAME"
ACTIVATE_DIR="$ENV_PREFIX/etc/conda/activate.d"

echo "==> Aplicando fix de PATH (prioriza el JDK de conda sobre SDKMAN/otros)"
mkdir -p "$ACTIVATE_DIR"
cat > "$ACTIVATE_DIR/env-vars.sh" << 'EOF'
# Prioriza el bin/ de este entorno conda por encima de gestores de JDK
# externos (SDKMAN, jenv, etc.) que puedan estar inyectados en el PATH.
export PATH="$CONDA_PREFIX/bin:$PATH"
EOF

echo ""
echo "==> Entorno '$ENV_NAME' creado correctamente."
echo ""
echo "Para empezar a usarlo:"
echo "  conda activate $ENV_NAME"
echo ""
echo "Verificación rápida recomendada tras activar:"
echo "  which java          # debe apuntar a \$CONDA_PREFIX/bin/java, no a SDKMAN/Temurin"
echo "  java -version        # debe mostrar openjdk 11.0.x"
echo "  python -c \"from pyflink.datastream import StreamExecutionEnvironment; print('PyFlink OK')\""
