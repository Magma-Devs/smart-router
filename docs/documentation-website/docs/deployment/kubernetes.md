# Kubernetes

Run Smart Router as a horizontally-scaled `Deployment` with a single `StatefulSet` (or `Deployment`) backing the shared cache. Router replicas are stateless and can be scaled independently.

## Topology

```
┌──────────────────────────────────────────────────┐
│  Service: smartrouter (ClusterIP / LoadBalancer) │
└────────────────────┬─────────────────────────────┘
                     │
   ┌─────────────────┼─────────────────┐
   ▼                 ▼                 ▼
┌────────┐       ┌────────┐       ┌────────┐
│ router │       │ router │  ...  │ router │   ← Deployment, replicas: N
└────┬───┘       └────┬───┘       └────┬───┘
     └──────────┬─────┴──────┬──────────┘
                ▼            ▼
         ┌─────────────────────────┐
         │  Service: smartrouter-  │
         │     cache (ClusterIP)   │
         └────────────┬────────────┘
                      ▼
                  ┌────────┐
                  │ cache  │   ← StatefulSet or Deployment, replicas: 1
                  └────────┘
```

## Cache StatefulSet

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: smartrouter-cache
spec:
  serviceName: smartrouter-cache
  replicas: 1
  selector: { matchLabels: { app: smartrouter-cache } }
  template:
    metadata: { labels: { app: smartrouter-cache } }
    spec:
      containers:
        - name: cache
          image: ghcr.io/magma-devs/smart-router:latest
          args: ["cache", "--port", "7778"]
          ports:
            - { name: cache, containerPort: 7778 }
          resources:
            requests: { cpu: 250m, memory: 256Mi }
            limits:   { cpu: "1",   memory: 1Gi }
---
apiVersion: v1
kind: Service
metadata:
  name: smartrouter-cache
spec:
  selector: { app: smartrouter-cache }
  ports:
    - { name: cache, port: 7778, targetPort: 7778 }
  clusterIP: None  # headless
```

## Router Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: smartrouter
spec:
  replicas: 3
  selector: { matchLabels: { app: smartrouter } }
  template:
    metadata: { labels: { app: smartrouter } }
    spec:
      containers:
        - name: router
          image: ghcr.io/magma-devs/smart-router:latest
          args:
            - rpcsmartrouter
            - /etc/smartrouter/config.yml
            - --geolocation=1
            - --use-static-spec=/etc/smartrouter/specs/
            - --cache-be=smartrouter-cache:7778
            - --log_level=info
          ports:
            - { name: rpc,     containerPort: 3360 }
            - { name: metrics, containerPort: 7779 }
          volumeMounts:
            - { name: config, mountPath: /etc/smartrouter, readOnly: true }
          resources:
            requests: { cpu: 500m, memory: 512Mi }
            limits:   { cpu: "2",  memory: 2Gi }
          readinessProbe:
            tcpSocket: { port: rpc }
            initialDelaySeconds: 5
            periodSeconds: 5
          livenessProbe:
            tcpSocket: { port: rpc }
            initialDelaySeconds: 30
            periodSeconds: 30
      volumes:
        - name: config
          projected:
            sources:
              - configMap: { name: smartrouter-config }
              - configMap: { name: smartrouter-specs }
---
apiVersion: v1
kind: Service
metadata:
  name: smartrouter
spec:
  selector: { app: smartrouter }
  ports:
    - { name: rpc,     port: 80,   targetPort: 3360 }
    - { name: metrics, port: 7779, targetPort: 7779 }
```

## ConfigMaps

Treat the router YAML and the `specs/` directory as configuration:

```bash
kubectl create configmap smartrouter-config \
  --from-file=config.yml=./config/smartrouter_examples/smartrouter_eth.yml

kubectl create configmap smartrouter-specs \
  --from-file=specs=./specs/
```

## Secrets

If your upstream URLs include API keys, put them in `Secret`s and reference them via env-var substitution in the YAML, or use a secret-aware config-rendering step in your pipeline.

## Horizontal autoscaling

Router replicas are stateless — `HorizontalPodAutoscaler` works:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata: { name: smartrouter }
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: smartrouter
  minReplicas: 3
  maxReplicas: 20
  metrics:
    - type: Resource
      resource: { name: cpu, target: { type: Utilization, averageUtilization: 70 } }
```

The cache is **not** autoscaled — it's a single replica. If you outgrow one cache instance, see [Cache → Shared state](../configuration/index.md).

## Observability

Annotate the router pods so Prometheus scrapes them:

```yaml
metadata:
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: "7779"
    prometheus.io/path: "/metrics"
```

For OpenTelemetry tracing, set the standard OTel env vars (`OTEL_EXPORTER_OTLP_ENDPOINT`, etc.) on the router container.

## Rollouts

The router supports zero-downtime rolling updates. Default `RollingUpdate` strategy with `maxUnavailable: 0` and a generous `terminationGracePeriodSeconds` (60s) lets in-flight relays drain before the pod exits.
