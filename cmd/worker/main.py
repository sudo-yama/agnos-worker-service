import time
import threading
import os
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer

from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

os.makedirs("logs", exist_ok=True)

#metrics
job_counter = Counter('worker_jobs_total', 'Total jobs processed')
job_duration = Histogram('worker_job_duration_seconds', 'Job processing time')

#readiness state
is_ready = False

#logging (console + file)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s|%(levelname)s|%(message)s",
    handlers=[
        logging.FileHandler("logs/jobs.log"),
        logging.StreamHandler()
    ]
)

#log access
access_logger = logging.getLogger("access")
access_logger.setLevel(logging.INFO)
access_handler = logging.FileHandler("logs/access.log")
access_handler.setFormatter(logging.Formatter("%(asctime)s|%(message)s"))
access_logger.addHandler(access_handler)

#worker loop
def worker_loop():
    global is_ready

    logging.info("Worker starting...")
    time.sleep(5)  # simulate init
    is_ready = True
    logging.info("Worker is ready")

    while True:
        start = time.time()

        logging.info("Processing job...")
        time.sleep(15)  # simulate work

        job_counter.inc()
        job_duration.observe(time.time() - start)

        logging.info("Job processed successfully")


# HTTP handler
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        
        access_logger.info(f"{self.client_address[0]} {self.command} {self.path}")
        
        if self.path == "/health/live":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"alive")

        elif self.path == "/health/ready":
            if is_ready:
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"ready")
            else:
                self.send_response(503)
                self.end_headers()

        elif self.path == "/metrics":
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(generate_latest())

        else:
            self.send_response(404)
            self.end_headers()


def run_server():
    server = HTTPServer(("0.0.0.0", 8080), Handler)
    logging.info("HTTP server running on :8080")
    server.serve_forever()


if __name__ == "__main__":
    t = threading.Thread(target=worker_loop)
    t.daemon = True
    t.start()

    run_server()