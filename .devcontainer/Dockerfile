FROM mcr.microsoft.com/vscode/devcontainers/base:0-bullseye

RUN apt-get update && \
    apt-get install -y redis-tools && \
    apt-get clean && rm -rf /var/lib/apt/lists/*