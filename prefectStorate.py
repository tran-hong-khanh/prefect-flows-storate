
from prefect import task, Flow, Parameter, unmapped
import prefect
from prefect.schedules import IntervalSchedule
from datetime import timedelta
import datetime
from prefect.agent.docker import DockerAgent
from prefect.storage import Local
from prefect.run_configs import UniversalRun
from prefect.storage import GitHub

@task
def get_numbers(n):
    return range(1, n + 1)


@task
def inc(x):
    return x + 1


@task
def add(x, y):
    return x + y


@task(log_stdout=True)
def compute_sum(nums):
    total = sum(nums)
    print(f"total = {total}")
    return total


with Flow("Mapping") as flow:
    # The number of branches in the mapped pipeline
    n = Parameter("n", default=3)

    # Generate the initial items to map over
    nums = get_numbers(n)  # [1, 2, 3]

    # Apply `inc` to every item in `nums`
    nums_2 = inc.map(nums)  # [2, 3, 4]

    # Apply `add` to every item in `nums_2`, with `2` as the second argument.
    nums_3 = add.map(nums_2, unmapped(2))  # [4, 5, 6]

    # Compute the sum of all items in `nums_3`
    total = compute_sum(nums_3)  # 15
    
# flow.run_config = UniversalRun(["vmg-B560M-AORUS-ELITE", "VMG"])

flow.storage = GitHub(
            repo="tran-hong-khanh/prefect-flows-storate",
            path="prefectStorate.py",
            # host="https://gitlab.vmgmedia.vn/",
            # access_token_secret="glpat-su5uaLifXGHDhJXQRnfM"
        )
