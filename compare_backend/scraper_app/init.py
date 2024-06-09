import sys
from flask import Flask

sys.path.append('/Tradepoint')
sys.path.append('/Screwfix')

from tradepoint_request import run_tradepoint
from screwfix_scraper import run_screwfix

app = Flask(__name__)

@app.route('/run_tasks')
def run_tasks():
    result1 = run_tradepoint()
    result2 = run_screwfix()
    return f"Tradepoint Request Done: {result1}, Screwfix Request Done: {result2}"

if __name__ == "__main__":
    app.run(debug=True)
