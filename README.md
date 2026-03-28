# MLOps Task

## Run locally
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log

## Run with Docker
docker build -t mlops-task .
docker run --rm mlops-task