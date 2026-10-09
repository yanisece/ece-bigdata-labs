# Lab — Object storage with S3

## Result

The reproducible users and orders datasets are generated with the project CLI and uploaded to the personal Onyxia bucket under `bronze/users.csv` and `bronze/orders.csv`.

The ingestion is executed by the Kubernetes Job in `kubernetes/job-upload-bronze.yaml`. Dataset files are mounted from a ConfigMap. S3 credentials are injected from a Secret and are never committed.

## Questions

1. **Why a Secret?** Credentials are confidential. A Secret separates them from non-sensitive configuration and allows tighter access control and rotation.
2. **What happens tomorrow?** Onyxia credentials are temporary, so an old Secret eventually causes authentication failures. Production workloads should use workload identity, a Kubernetes service account bound to an IAM role, or a secret manager with automatic rotation.
3. **Daily ingestion?** Convert the Job template into a Kubernetes CronJob with a daily schedule, an appropriate concurrency policy and retained execution history.

## Security

Generated datasets, local AWS configuration, environment files and credentials remain outside Git.
