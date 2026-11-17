FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY lakehouse_demo ./lakehouse_demo
COPY config ./config
ENTRYPOINT ["python", "-m", "lakehouse_demo.cli"]
CMD ["demo", "--output", "/data/demo"]
