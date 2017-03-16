FROM rabbitmq@sha256:60fa2366e203f1515b99082c4a2f3dcb157d836eacedb31dfdeb97d3fd9dd1ee
MAINTAINER Justin Littman <justinlittman@gwu.edu>

ADD start.sh /usr/local/bin/mq_start.sh
RUN chmod +x /usr/local/bin/mq_start.sh

ENV RABBITMQ_MNESIA_BASE /sfm-data/rabbitmq
ENV RABBITMQ_LOG_BASE /sfm-data/rabbitmq

ENTRYPOINT ["/usr/local/bin/mq_start.sh"]