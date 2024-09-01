from fastapi import FastAPI, File, UploadFile, HTTPException
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
app = FastAPI()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: v.encode('utf-8')
)

@app.post("/upload/")
async def upload_file(language: str, file: UploadFile = File(...)):
    if language not in ["python", "c++"]:
        raise HTTPException(status_code=400, detail="Unsupported language")
    try:
        code_content = await file.read()
        code_str = code_content.decode('utf-8')
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read file: {str(e)}")
    topic = 'source_codes_app'

    try:
        future = producer.send(topic, value=json.dumps({"code" : code_str, "language" : language}))
        result = future.get(timeout=10)
    except KafkaError as e:
        raise HTTPException(status_code=500, detail=f"Failed to send message to Kafka: {str(e)}")

    return {"status": 201, "message": "Code successfully sent to Kafka"}
