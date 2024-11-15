flowchart TB
subgraph "10k+ Events Scenario"
E1[Events Coming In] --> K1[Kafka Topic]
K1 --> |Partition 1| P1[Pod 1]
K1 --> |Partition 2| P2[Pod 2]
K1 --> |Partition 3| P3[Pod 3]
end

    subgraph "Redis Cluster"
        direction LR
        RM1[Redis Master 1] --> RS1[Redis Slave 1]
        RM2[Redis Master 2] --> RS2[Redis Slave 2]
        RM3[Redis Master 3] --> RS3[Redis Slave 3]

        subgraph "Counter Storage"
            C1["202311151442: 9999"]
            C2["202311151443: 4521"]
            C3["202311151441: 7832"]
        end
    end

    P1 & P2 & P3 --> RM1 & RM2 & RM3
