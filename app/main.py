from fastapi import FastAPI
from generator import generate_gps_data

app = FastAPI(
    title="Real-Time Logistics API",
    version="1.0.0"
)


@app.get("/")
def health_check():
    return {"status": "running"}


@app.get("/gps")
def get_gps_data():
    return generate_gps_data()


@app.get("/gps/bulk")
def get_bulk_gps_data(records: int = 10):
    return [generate_gps_data() for _ in range(records)]