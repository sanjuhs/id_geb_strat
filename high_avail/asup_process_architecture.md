flowchart TB
subgraph "External System"
ES[ASUP Event Publisher]
end

    subgraph "Kafka Input"
        KT1[Input Kafka Topic]
    end

    subgraph "Processing Pipeline"
        direction TB
        EC[Event Consumer Router]
        EP[Event Processor]
        IG[ASUP ID Generator]
        SB[State Backup Manager]
        PR[Event Producer Router]

        EC --> EP
        EP --> IG
        IG <--> SB
        IG --> PR
    end

    subgraph "Storage"
        Redis[(Redis Cluster)]
        S3[(AWS S3)]
        KT2[Output Kafka Topic]
    end

    ES --> KT1
    KT1 --> EC
    IG <--> Redis
    PR --> KT2
    PR --> S3
