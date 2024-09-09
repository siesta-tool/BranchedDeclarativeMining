#Create multiple datasets from a single logfile, where some traces might continue from the previous logfile. And each log will have approximately as many events as the original dataset but shrinked to fit into the "want_days" parameter.

docker build -t multiple_datasets .

docker run --mount type=bind,source=$(pwd)/input,target=/app/input \
--mount type=bind,source=$(pwd)/output,target=/app/output \
multiple_datasets input/log_100_113.xes 10 2