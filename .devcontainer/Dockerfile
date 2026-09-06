FROM mcr.microsoft.com/devcontainers/base:bookworm

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

ENV PATH=/usr/local/cargo/bin:${PATH}

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        wget \
        gnupg \
        git \
        gh \
        openssh-client \
        sudo \
        less \
        procps \
        lsof \
        vim \
        nano \
        unzip \
        jq \
        systemd

# Networking / diagnostics
RUN apt-get install -y --no-install-recommends \
        iputils-ping \
        iproute2 \
        dnsutils \
        netcat-openbsd \
        tcpdump \
        traceroute \
        telnet \
        net-tools

RUN apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

CMD ["tail", "-f", "/dev/null"]
