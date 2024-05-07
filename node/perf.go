package node

const template = `orderer-client:
  broadcast: ["{ORDERER1}", "{ORDERER2}", "{ORDERER3}", "{ORDERER4}"]
  deliver: ["{ASSEMBLER}"]
  channel-id: arma
  reconnect: 2s
  connection-profile: {"root-ca-paths": ["{TLSCACERTS}"], "msp-dir": "", "msp-id": "", "bccsp": {"Default": "SW", "SW": {"Security": 256, "Hash": "SHA2"}}}
  type: BFT
  signed-envelopes: False
  parallelism: 1
message-profile:
  workers: 3
  input-channel-capacity: 20
  messages: 100
  message-size: 160
  message-start: 0
logging:
  enabled: true
  level: INFO
  development: false
monitoring:
  metrics:
    enable: true
    endpoint: :2119
    latency:
      sampler:
        type: timer
        sampling-interval: 10s
      buckets:
        type: uniform
        max-latency: 5s
        bucket-count: 1000
rate-limit:
  endpoint: :6997
  initial-limit: 10`
