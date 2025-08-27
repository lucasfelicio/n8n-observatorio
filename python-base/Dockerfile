FROM python:3.11-slim

# Dependências do sistema (Java 21, ps, build tools, tini)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless procps curl tini \
    build-essential gcc g++ \
  && rm -rf /var/lib/apt/lists/*

# Criar symlinks que o Spark costuma procurar (java-21 e java-11)
RUN set -eux; \
    if command -v java >/dev/null 2>&1; then \
      REAL_JAVA_DIR="$(readlink -f $(command -v java) | sed 's:/bin/java::')"; \
      mkdir -p /usr/lib/jvm; \
      # link para java-21 (se já não existir)
      ln -sf "${REAL_JAVA_DIR}" /usr/lib/jvm/java-21-openjdk-amd64; \
      # e link também para java-11 (muitos scripts do spark procuram este caminho)
      ln -sf "${REAL_JAVA_DIR}" /usr/lib/jvm/java-11-openjdk-amd64; \
    fi

# Defina JAVA_HOME para o caminho que Spark costuma usar (mantemos java-11 por compatibilidade)
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$PATH:/root/.local/bin"

# Evitar avisos do Python
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Bibliotecas Python 
RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    delta-spark==3.2.0 \
    deltalake==0.17.2 \
    fastapi==0.115.0 \
    uvicorn[standard]==0.30.6 \
    streamlit==1.38.0 \
    pandas \
    pyarrow==17.0.0 \
    python-dotenv \
    requests \
    sqlalchemy \
    duckdb \
    netifaces \
    pydantic

# Configurações gerais
WORKDIR /app
RUN useradd -m -u 1000 appuser
USER appuser
EXPOSE 8000 8501
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["bash", "-lc", "sleep infinity"]