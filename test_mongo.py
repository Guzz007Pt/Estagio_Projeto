import os
from datetime import datetime, timezone
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()  # <--- this loads .env in the project folder

uri = os.getenv("MONGO_URI")
if not uri:
    raise RuntimeError("MONGO_URI is missing. Add it to your .env file.")

client = MongoClient(uri, serverSelectionTimeoutMS=5000)

client.admin.command("ping")
print("✅ MongoDB ping OK")

col = client["meteo"]["meteo"]
res = col.insert_one({"fonte": "TEST", "data": datetime.now(timezone.utc), "ok": True})
print("✅ Insert OK:", res.inserted_id)

client.close()
