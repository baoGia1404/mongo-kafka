FROM confluentinc/cp-kafka-connect:latest

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"

RUN  confluent-hub install --no-prompt confluentinc/kafka-connect-datagen:latest
RUN  confluent-hub install --no-prompt confluentinc/kafka-connect-rabbitmq:latest