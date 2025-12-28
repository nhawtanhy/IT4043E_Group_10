

## IT4043E_Group_10

End-to-End Big Data Platform for Real-Time Weather Analytics

This project implements an end-to-end big data pipeline for collecting, processing, and analyzing real-time weather data in Vietnam.
The system integrates Apache Kafka, Apache Spark (streaming & batch), distributed storage, and machine learning inference, all orchestrated on Kubernetes.

# Run in folder Kubernetes
⸻

Kubernetes Cluster Configuration

kubectl get nodes -o wide
kubectl get namespaces -o name

Note: The system runs on a single-node Kubernetes cluster (Kind), where the node acts as both control plane and worker.

⸻

Kafka Layer

Clean up image if needed

(Remember to reload the image into Kind after removal)

docker exec -it data-platform-control-plane crictl rmi weather-kafka-producer:latest

Build and load producer image

docker build -t weather-kafka-producer:latest .
kind load docker-image weather-kafka-producer:latest --name data-platform

Create OpenWeather API secret

kubectl create secret generic openweather-secret \
  --from-literal=api-key=<YOUR_API_KEY> \
  -n data

Deploy Kafka (Strimzi)

kubectl apply -f kafka-nodepool.yaml
kubectl apply -f kafka.yaml

Check Kafka producer

kubectl get pods -n data -w
kubectl logs -n data deploy/weather-kafka-producer
kubectl get pods -n data | grep kafka

Check Kafka topics and consumer groups

kubectl exec -n data -it kafka-kafka-pool-1 -- \
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

kubectl exec -n data -it kafka-kafka-pool-1 -- \
/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

kubectl exec -n data -it kafka-kafka-pool-1 -- \
/opt/kafka/bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--describe --topic weather-raw

Inspect Kafka broker pod

kubectl describe pod -n data kafka-kafka-pool-0

Consume messages manually (debug)

kubectl exec -it -n data kafka-kafka-pool-0 -- bash

/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-kafka-bootstrap.data.svc.cluster.local:9092 \
  --topic weather-raw

Delete producer pod (restart test)

kubectl delete pod -l app=weather-kafka-producer


⸻

Spark Streaming (Kafka → Bronze)

Delete existing Spark application

kubectl delete sparkapplication weather-streaming-to-bronze -n default

Clear streaming checkpoint

kubectl exec -it checkpoint-debug -- sh
ls -lh /checkpoint
rm -rf /checkpoint/weather-raw

Rebuild streaming image

docker exec -it data-platform-control-plane crictl rmi spark-weather-stream:3.3.3-r8
docker build -t spark-weather-stream:3.3.3-r8 .
kind load docker-image spark-weather-stream:3.3.3-r8 --name data-platform

Deploy streaming job

kubectl apply -f k8s/weather-streaming-to-bronze.yaml

Check streaming logs

kubectl logs -n default weather-streaming-to-bronze-driver --tail=200
kubectl logs -n default weather-streaming-to-bronze-driver -f


⸻

Bronze Batch Layer (Parquet → Elasticsearch)

Delete cronjob and old jobs

kubectl delete cronjob bronze-to-es-batch-cron
kubectl delete job -l job-name=bronze-to-es

Clear batch state

kubectl exec -it checkpoint-debug -- sh
rm -rf /checkpoint/batch_state

Rebuild Bronze batch image

docker exec -it data-platform-control-plane crictl rmi spark-weather-bronze:3.3.3-r6
docker build -t spark-weather-bronze:3.3.3-r6 .
kind load docker-image spark-weather-bronze:3.3.3-r6 --name data-platform

Deploy batch SparkApplication

kubectl apply -f k8s/bronze-to-es-batch.yaml

Trigger cronjob manually (testing)

kubectl create job bronze-to-es-manual \
  --from=cronjob/bronze-to-es-batch-cron

Debug batch execution

kubectl get sparkapplication
kubectl describe pod -n default bronze-to-es-batch-*-exec-1 | sed -n '/Events:/,$p'
kubectl logs -f bronze-to-es-batch-*-driver


⸻

S3 Historical Data Ingestion

Build and load image

docker exec -it data-platform-control-plane crictl rmi weather-s3-history:latest
docker build -t weather-s3-history:latest .
kind load docker-image weather-s3-history:latest --name data-platform

Create AWS credentials secret

kubectl create secret generic aws-secret \
  -n data \
  --from-literal=access-key=<ACCESS_KEY> \
  --from-literal=secret-key=<SECRET_KEY>

Trigger cronjob manually

kubectl create job \
  --from=cronjob/weather-history-to-s3 \
  weather-history-manual \
  -n data

Verify data on S3

aws s3 ls s3://hust-bucket-storage/weather_silver/


⸻

Silver Layer (S3 → Parquet)

Build image and deploy

docker build -t spark-weather-silver:3.3.3-r2 .
kind load docker-image spark-weather-silver:3.3.3-r2 --name data-platform

kubectl apply -f silver-pvc.yaml
kubectl apply -f silver-from-s3.yaml

Debug Silver jobs

kubectl logs -n default weather-s3-to-silver-*-driver --tail=300
kubectl describe sparkapplication weather-s3-to-silver-*


⸻

Gold Layer (Aggregations → Elasticsearch)

docker build -t spark-weather-gold:3.3.3 .
kind load docker-image spark-weather-gold:3.3.3 --name data-platform

kubectl apply -f gold-pvc.yaml
kubectl apply -f silver-to-gold-es.yaml


⸻

Machine Learning Inference

docker build -t spark-weather-ml:3.3.3-v1 .
kind load docker-image spark-weather-ml:3.3.3-v1 --name data-platform

kubectl apply -f ml.yaml
kubectl logs -f weather-ml-inference-*-driver


⸻

Elasticsearch & Kibana

Elasticsearch

kubectl get elasticsearch -n observability
kubectl port-forward -n observability svc/es-weather-es-http 9200:9200

Get elastic password:

kubectl get secret es-weather-es-elastic-user -n observability \
  -o jsonpath='{.data.elastic}' | base64 --decode

Kibana

kubectl apply -f kibana.yaml
kubectl port-forward -n observability svc/kibana-weather-kb-http 5601:5601


⸻

Monitoring (Prometheus & Grafana)

helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

kubectl create namespace monitoring
helm install monitoring prometheus-community/kube-prometheus-stack -n monitoring

Grafana password:

kubectl get secret -n monitoring monitoring-grafana \
  -o jsonpath="{.data.admin-password}" | base64 --decode

Cleanup:

helm uninstall monitoring -n monitoring
kubectl delete namespace monitoring


⸻

Resource Inspection & Cleanup

kubectl describe node data-platform-control-plane
kubectl get sparkapplication -n default

Bulk delete Spark applications:

kubectl delete sparkapplication -n default \
  $(kubectl get sparkapplication -n default \
    | egrep 'bronze-to-es-batch|weather-s3-to-silver|silver-to-gold-es|weather-ml' \
    | awk '{print $1}')


