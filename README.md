# Livy Hich Concurrency

Creates a HC session with Livy and sees what happens

- [High concurrency support in the Fabric Livy API](https://learn.microsoft.com/en-us/fabric/data-engineering/high-concurrency-livy)
- [Regular Hich concurrency support in Fabric Spark](https://learn.microsoft.com/en-us/fabric/data-engineering/high-concurrency-overview#dynamic-session-sharing-limit)

## Pre-reqs

Create a Fabric Workspace:

![Fabric Workspace](.imgs/workspace.png)

Create a Pool - say Large so you can fire 16 on all 16 cylinders:

![Large Pool](.imgs/pool.png)

Create an Environment, attach the pool to it

![Environment](.imgs/environment.png)

Then add `spark.highConcurrency.max` = 50, we want to run 50 concurrenct queries in this single pool:

![Concurrency](.imgs/concurrency.png)


## Run

```powershell
$GIT_ROOT = git rev-parse --show-toplevel

# Setup
#
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt

python test.py --namespace "arcdataanalyticsmirrormaker" --topic "delta-bulk-loader" --wait-in-seconds "300```