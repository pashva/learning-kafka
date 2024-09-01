from kafka import KafkaConsumer
import docker
import os
import json
import time
# Initialize Docker client
docker_client = docker.from_env()

def run_code_in_docker(language, code):

    if language == 'python':
        image = 'python-kafka'
        file_name = 'temp_code.py'
    elif language == 'c++':
        image = 'cpp-runner'
        file_name = 'temp_code.cpp'
    else:
        raise ValueError("Unsupported language")
    dockerfile_python = f"""
    FROM python:3.11-slim

    WORKDIR /app

    # Copy the Python code into the container
    COPY temp_code.py /app/temp_code.py

    # Run the Python script
    CMD ["sh", "-c", "python3 temp_code.py"]
    """
    with open("temp/" + file_name, 'w') as file:
        file.write(code)
    with open("temp/Dockerfile", 'w') as file:
        file.write(dockerfile_python)
    try:
        image, logs = docker_client.images.build(
            path="temp",
            tag='dynamic-python-image',
            rm=True
        )
        container = docker_client.containers.run(
            image='dynamic-python-image',
            detach=True,
        )
        container.wait()
        output = container.logs().decode('utf-8')
        container.remove(force=True)
        docker_client.images.remove(image='dynamic-python-image', force=True)
        # container = docker_client.containers.run(image, detach=True, volumes={'C:/Users/mehta/Desktop/myworks/learning-kafka/temp': {'bind' : '/app/temp','mode': 'rw'}})
        # exec_result = container.exec_run("python3 temp/temp_code1.py")
        # if exec_result.exit_code == 0:
        #     output = exec_result.output.decode('utf-8')
        #     container.kill()
        #     container.remove()
    except Exception as e:
        output = str(e)
    finally:
        if os.path.exists("temp/" + file_name):
            os.remove("temp/" + file_name)
            os.remove("temp/Dockerfile")

    return output

def main():
    consumer = KafkaConsumer(
        'source_codes_app',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id='code_executors',
        enable_auto_commit=True
    )

    for message in consumer:
        try:
            deserialized_msg = json.loads(message.value)
            result = run_code_in_docker(deserialized_msg['language'], deserialized_msg['code'])
            print(f"Execution result:\n{result}\n")

        except Exception as e:
            print(f"Error processing message: {str(e)}")

if __name__ == "__main__":
    main()
