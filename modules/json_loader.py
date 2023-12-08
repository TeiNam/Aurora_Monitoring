import json
import os
from fastapi import HTTPException

BASE_DIR = os.path.join(os.path.dirname(__file__), '..')


def load_json(filename):
    full_path = os.path.join(BASE_DIR, filename)
    try:
        with open(full_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File {filename} not found")


def save_json(filename, data):
    full_path = os.path.join(BASE_DIR, filename)
    try:
        with open(full_path, "w") as f:
            json.dump(data, f, indent=4)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error saving to file: {e}")
