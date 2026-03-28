import argparse
import pandas as pd
import numpy as np
import yaml
import json
import logging
import time
import sys

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input")
    parser.add_argument("--config")
    parser.add_argument("--output")
    parser.add_argument("--log-file")
    args = parser.parse_args()

    logging.basicConfig(filename=args.log_file, level=logging.INFO)

    start_time = time.time()

    try:
        logging.info("Job started")

        # Load config
        with open(args.config, 'r') as f:
            config = yaml.safe_load(f)

        # Validate config
        if not all(k in config for k in ["seed", "window", "version"]):
            raise ValueError("Invalid config structure")

        seed = config["seed"]
        window = config["window"]
        version = config["version"]

        np.random.seed(seed)

        logging.info(f"Config loaded: {config}")

        # Load data
        df = pd.read_csv(args.input, sep=",", engine="python")

        if df.empty:
            raise ValueError("Empty dataset")

        if "close" not in df.columns:
            raise ValueError("Missing 'close' column")

        logging.info(f"Rows loaded: {len(df)}")

        # Rolling mean
        df["rolling_mean"] = df["close"].rolling(window=window).mean()

        # Signal
        df["signal"] = (df["close"] > df["rolling_mean"]).astype(int)

        # Remove NaN rows
        df = df.dropna()

        rows_processed = len(df)
        signal_rate = df["signal"].mean()

        latency_ms = int((time.time() - start_time) * 1000)

        metrics = {
            "version": version,
            "rows_processed": rows_processed,
            "metric": "signal_rate",
            "value": round(signal_rate, 4),
            "latency_ms": latency_ms,
            "seed": seed,
            "status": "success"
        }

        logging.info(f"Metrics: {metrics}")

    except Exception as e:
        metrics = {
            "version": "v1",
            "status": "error",
            "error_message": str(e)
        }
        logging.error(str(e))
        sys.exit(1)

    finally:
        with open(args.output, "w") as f:
            json.dump(metrics, f, indent=4)

        print(json.dumps(metrics, indent=4))
        logging.info("Job finished")

if __name__ == "__main__":
    main()