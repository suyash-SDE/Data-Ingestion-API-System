# Data-Ingestion-API-System




This is a RESTful API system built with Node.js + Express that allows clients to submit ingestion jobs and check their processing status. The system supports batching, asynchronous processing, rate-limiting, and priority-based execution.

---

 Features

- **POST `/ingest`: Submit a list of IDs and priority.
- **GET `/status/:ingestion_id`: Check the status of the submitted job.
- Jobs are split into batches of 3 IDs per batch.
- Batches are processed **asynchronously** with a rate limit of 1 batch every 5 seconds**.
- Supports priorities**: `HIGH > MEDIUM > LOW`, with FIFO for same priority.

---

 Hosted URL

 Base URL: https://github.com/suyash-SDE/Data-Ingestion-API-System.git


> Replace with your actual deployed link** (e.g., Render, Railway, etc.)

---

Ingestion API

 Endpoint
