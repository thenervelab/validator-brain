#!/usr/bin/env python3
"""
Development server for IPFS Service Validator.

This script runs the FastAPI application with hot-reloading enabled.
"""
import uvicorn

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)