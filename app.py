from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from backend_sent_mails import process_sent_mails

app = Flask(__name__)

@app.route('/')
def index():
    return 'sent mails'

print("Scheduler is called.")
sched = BackgroundScheduler(daemon=False)
sched.add_job(process_sent_mails, 'interval', seconds=60, max_instances=1)
sched.start()



