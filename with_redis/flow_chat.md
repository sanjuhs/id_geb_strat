# ASUP Processing System Documentation

## 1. High-Level System Overview

```mermaid
graph TB
    subgraph "Input Sources"
        S["Servers"] --> KI["Kafka Input Topic"]
    end

    subgraph "ASUP Processing System"
        KI --> P["ASUP Processor"]
        P <--> R["Redis\n(ID Coordination)"]
        P --> KO["Kafka Output Topic"]
    end

    subgraph "Downstream Systems"
        KO --> DA["Data Analysis"]
        KO --> M["Monitoring"]
        KO --> O["Other Systems"]
    end

    classDef primary fill:#2ecc71,stroke:#27ae60,color:white
    classDef secondary fill:#3498db,stroke:#2980b9,color:white
    classDef storage fill:#95a5a6,stroke:#7f8c8d,color:white

    class P primary
    class R,KI,KO storage
    class DA,M,O secondary
```

## 2. ASUP ID Generation Process

```mermaid
flowchart TD
    A[Start ID Generation] --> B{Try Current Minute}
    B -->|Success| C[Return ASUP ID\nYYYYMMDDHHMM####]
    B -->|Full| D{Try -1 Minute}
    D -->|Success| C
    D -->|Full| E{Try +1 Minute}
    E -->|Success| C
    E -->|Full| F[Continue ±10 mins]
    F -->|Found Space| C
    F -->|All Full| G[Log Error]

    style A fill:#2ecc71,stroke:#27ae60,color:white
    style C fill:#3498db,stroke:#2980b9,color:white
    style G fill:#e74c3c,stroke:#c0392b,color:white
```

## 3. Redundancy and Fault Tolerance

```mermaid
graph TB
    subgraph "High Availability Setup"
        subgraph "Instance 1"
            P1[Processor 1] --> R1[Redis Primary]
        end

        subgraph "Instance 2"
            P2[Processor 2] --> R1
        end

        subgraph "Instance 3"
            P3[Processor 3] --> R1
        end

        R1 --> R2[Redis Replica 1]
        R1 --> R3[Redis Replica 2]
    end

    style P1,P2,P3 fill:#2ecc71,stroke:#27ae60,color:white
    style R1 fill:#e67e22,stroke:#d35400,color:white
    style R2,R3 fill:#f1c40f,stroke:#f39c12,color:white
```

## 4. Message Processing Flow

```mermaid
sequenceDiagram
    participant S as Server
    participant KI as Kafka Input
    participant P as Processor
    participant R as Redis
    participant KO as Kafka Output

    S->>KI: Send ASUP
    KI->>P: Consume Message
    P->>R: Request ID
    R-->>P: Return Sequence Number
    P->>P: Generate ASUP ID
    P->>KO: Publish Processed ASUP
    Note over P,KO: Include generated ID
```

## 5. Testing Architecture

```mermaid
graph TB
    subgraph "Test Suite Components"
        UT[Unit Tests] --> TG[Test Generator]
        IT[Integration Tests] --> TG
        LT[Load Tests] --> TG

        TG --> TR[Test Runner]

        subgraph "Mock Components"
            MR[Mock Redis]
            MK[Mock Kafka]
            MM[Mock Messages]
        end

        TR --> Rep[Test Reports]
    end

    style UT,IT,LT fill:#3498db,stroke:#2980b9,color:white
    style TR fill:#2ecc71,stroke:#27ae60,color:white
    style MR,MK,MM fill:#95a5a6,stroke:#7f8c8d,color:white
```

## 6. Load Testing Scenarios

```mermaid
graph LR
    subgraph "Load Test Types"
        B[Baseline\n1k/min] --> N[Normal Load\n5k/min]
        N --> H[High Load\n10k/min]
        H --> S[Spike Load\n15k/min]

        subgraph "Metrics Monitored"
            RT[Response Time]
            TH[Throughput]
            ER[Error Rate]
            DU[Duplicate IDs]
        end
    end

    style B,N fill:#2ecc71,stroke:#27ae60,color:white
    style H fill:#f1c40f,stroke:#f39c12,color:white
    style S fill:#e74c3c,stroke:#c0392b,color:white
```

## 7. Error Handling Flow

```mermaid
flowchart TD
    A[Error Occurs] --> B{Error Type}
    B -->|Redis Error| C[Retry Connection]
    B -->|Kafka Error| D[Message Recovery]
    B -->|ID Generation| E[Try Different Minute]

    C -->|Success| F[Resume Processing]
    C -->|Failure| G[Alert Operations]

    D -->|Success| F
    D -->|Failure| G

    E -->|Success| F
    E -->|Failure| G

    style A fill:#e74c3c,stroke:#c0392b,color:white
    style F fill:#2ecc71,stroke:#27ae60,color:white
    style G fill:#e67e22,stroke:#d35400,color:white
```

## 8. Scaling Strategy

```mermaid
graph TB
    subgraph "Kafka Partitioning"
        P1[Partition 1] --> PR1[Processor 1]
        P2[Partition 2] --> PR2[Processor 2]
        P3[Partition 3] --> PR3[Processor 3]
    end

    subgraph "Redis Cluster"
        PR1 & PR2 & PR3 --> |ID Generation| RC[Redis Cluster]
    end

    style P1,P2,P3 fill:#3498db,stroke:#2980b9,color:white
    style PR1,PR2,PR3 fill:#2ecc71,stroke:#27ae60,color:white
    style RC fill:#e67e22,stroke:#d35400,color:white
```

## Key Features Explained

1. **ID Generation**:

   - First 12 digits: YYYYMMDDHHMM (timestamp)
   - Last 4 digits: Sequential number (0000-9999)
   - Flexible minute allocation (±10 minutes)

2. **Fault Tolerance**:

   - Multiple processor instances
   - Redis replication
   - Kafka partition redundancy
   - Automatic error recovery

3. **Load Handling**:

   - Supports >10k ASUPs per minute
   - Distributes load across minutes
   - Multiple processing instances
   - Kafka partitioning

4. **Testing Coverage**:
   - Unit tests for components
   - Integration tests for workflow
   - Load tests for performance
   - Mock components for isolation
