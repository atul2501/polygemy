FROM postgres:17.6-bookworm
COPY scripts/backup.sh /backup.sh
ENTRYPOINT ["bash", "/backup.sh"]
CMD ["loop"]
