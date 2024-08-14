from prometheus_client import Gauge, start_http_server

class PrometheusMetrics:
    def __init__(self):
        self.scraping_progress = Gauge('scraping_progress', 'Progress of scraping job', ['module'])
        self.total_jobs = Gauge('total_jobs', 'Total number of jobs for scraping', ['module'])
        self.jobs_remaining = Gauge('jobs_remaining', 'Number of remaining jobs for scraping', ['module'])

    def set_total_jobs(self, module, total):
        self.total_jobs.labels(module=module).set(total)

    def update_progress(self, module, progress):
        self.scraping_progress.labels(module=module).set(progress)

    def update_jobs_remaining(self, module, remaining):
        self.jobs_remaining.labels(module=module).set(remaining)

def initialize_prometheus_server(port=8000):
    start_http_server(port)
    return PrometheusMetrics()