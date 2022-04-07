
from prefect import task, Flow
import prefect
from prefect.schedules import IntervalSchedule
from datetime import timedelta
import datetime
from prefect.agent.docker import DockerAgent
from prefect.storage import Local
from prefect.run_configs import UniversalRun
from prefect.storage import GitHub

@task
def hello_task():
    logger = prefect.context.get("logger")
    logger.info("Hello world!")
    
# schedule to run every 2 minutes
schedule = IntervalSchedule(
    start_date=datetime.datetime.utcnow() + timedelta(seconds=1),
    interval=timedelta(minutes = 2))

# define Prefect flow
# with Flow("hello-flow", schedule=schedule, run_config=UniversalRun()) as flow:
with Flow("hello-flow", schedule=schedule) as flow:
    hello_task()
flow.run_config = UniversalRun(["vmg-B560M-AORUS-ELITE"])
flow.storage = GitHub(
            repo="tran-hong-khanh/prefect-flows-storate",
            path="prefectStorate.py",
            # host="https://gitlab.vmgmedia.vn/",
            # access_token_secret="glpat-su5uaLifXGHDhJXQRnfM"
        )
