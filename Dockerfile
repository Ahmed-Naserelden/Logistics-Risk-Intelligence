FROM python:latest

RUN apt update && apt install -y cron && apt install -y nano sudo && apt install -y openssh-server


RUN addgroup orca && \
    adduser --disabled-password --gecos "" --ingroup orca orca && \
    echo "orca ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

RUN mkdir -p /home/orca/.ssh && \
    ssh-keygen -t rsa -N "" -f /home/orca/.ssh/id_rsa && \
    cat /home/orca/.ssh/id_rsa.pub >> /home/orca/.ssh/authorized_keys && \
    chmod 600 /home/orca/.ssh/authorized_keys && \
    chown -R orca:orca /home/orca/.ssh

ENTRYPOINT ["bash", "-c", "cron -f"]