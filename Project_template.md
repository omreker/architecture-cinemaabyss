# Задание 1

[CinemaAbyss Container Diagram](diagrams/container/cinemaabyss_container.puml)

# Задание 2

## 1. Proxy

![CinemaAbyss API Tests](screenshots\cinemaabyss_api_tests.png)

### MOVIES_MIGRATION_PERCENT: "20"

![Proxy Service Test](screenshots\proxy_service_test.png)

## 2. Kafka

### Kafka Tests

![Kafka Tests](screenshots\kafka_tests.png)

### Kafka Topics

![Kafka Topics](screenshots\kafka_topics.png)

# Задание 3

## CI/CD

![CI/CD](screenshots\ci_cd.png)

## Proxy в Kubernetes

## Helm

![API Movies](screenshots\api_movies.png)

## Event Service Tests

![Event Service Tests](screenshots\event_service_tests.png)

## Event Service Logs

![Event Service Logs](screenshots\event_service_logs.png)

# Задание 4

## Helm

![Helm](screenshots\helm.png)

## API Movies after Helm releases

![API Movies](screenshots\api_movies_2.png)

## Удаляем все

```bash
kubectl delete all --all -n cinemaabyss
kubectl delete namespace cinemaabyss
```
