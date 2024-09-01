from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id='code_execution_engines'
)

topic_name = "source_codes_app"
partitions = 1
replication_factor = 1

topic = NewTopic(name=topic_name, num_partitions=partitions, replication_factor=replication_factor)

admin_client.create_topics(new_topics=[topic], validate_only=False)

print(f"Topic '{topic_name}' created with {partitions} partitions.")

admin_client.close()