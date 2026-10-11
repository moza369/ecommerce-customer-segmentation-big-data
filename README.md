# E-commerce Customer Segmentation with Apache Spark and Kafka

A Big Data project combining batch customer segmentation, clustering-algorithm comparison, and a Kafka-based streaming demonstration. The batch pipeline builds Recency, Frequency, and Monetary (RFM) features and applies clustering with Apache Spark. A second notebook consumes transaction events from Kafka and associates recognized customers with their previously computed segment.

## Project overview

- **Big Data processing:** Apache Spark and PySpark DataFrames, with educational RDD operations.
- **Customer analytics:** RFM feature engineering.
- **Machine learning:** Log transformation, feature assembly, standardization, and K-Means clustering with Spark MLlib.
- **Model comparison:** K-Means vs. Agglomerative Clustering (Ward linkage).
- **Evaluation:** Silhouette score using squared Euclidean distance.
- **Streaming:** Kafka and Spark Structured Streaming.
- **Environment:** Docker Compose and Jupyter.

## Workflow

1. Load and explore the Online Retail dataset.
2. Clean duplicate records, rows without customer IDs, cancellation invoices, and transactions with non-positive quantities or prices.
3. Calculate transaction revenue and aggregate customer-level RFM features.
4. Apply log transformations and standardize the RFM features.
5. Evaluate K-Means for K values from 2 through 8.
6. Compare K-Means and Agglomerative Clustering (Ward) using two clusters and silhouette scores with squared Euclidean distance.
7. Select the algorithm with the higher comparison score, inspect customer profiles, and save the selected segmentation as Parquet.
8. Consume JSON transactions from Kafka and associate recognized customers with their saved segment.

The streaming notebook uses the saved segmentation; it does not retrain the clustering model. Customers absent from the saved segmentation may have a null cluster.

## Results

The original Online Retail dataset contains **541,909 rows**. After preprocessing, the notebook reports approximately **392,692 valid transactions**, **4,338 customers**, **18,532 invoices**, and **3,665 products**.

### K-Means model selection

The Spark MLlib K-Means section evaluates K from 2 through 8 using Spark's `ClusteringEvaluator` with `distanceMeasure="squaredEuclidean"`.

| K | Silhouette score | Training cost |
|---:|---:|---:|
| 2 | 0.6238 | 6486.51 |
| 3 | 0.5013 | 4878.00 |
| 4 | 0.4999 | 3961.85 |
| 5 | 0.4772 | 3330.25 |
| 6 | 0.4704 | 2874.82 |
| 7 | 0.4379 | 2595.93 |
| 8 | 0.4398 | 2361.80 |

Among the tested values, **K = 2** has the highest silhouette score.

### Clustering algorithm comparison

K-Means and Agglomerative Clustering (Ward linkage) are compared using the same standardized RFM feature vectors, two clusters, and scikit-learn's silhouette score with `metric="sqeuclidean"`.

| Algorithm | Clusters | Silhouette score |
|---|---:|---:|
| K-Means | 2 | **0.6238** |
| Agglomerative Clustering (Ward) | 2 | 0.5567 |

K-Means achieves the higher silhouette score in this comparison and is selected for the saved segmentation. Both evaluation steps use squared Euclidean distance; the K-Means model-selection score is calculated by Spark MLlib, while the algorithm-comparison scores are calculated with scikit-learn.

### Customer profiles

The selected K-Means segmentation produces these descriptive profiles:

| Cluster | Customers | Share | Average recency (days) | Average frequency (invoices) | Average monetary value |
|---|---:|---:|---:|---:|---:|
| 0 | 1,654 | 38.13% | 25.39 | 8.49 | 4,566.18 |
| 1 | 2,684 | 61.87% | 133.15 | 1.68 | 497.30 |

Cluster 0 contains customers with more recent purchases and higher average frequency and monetary value. Cluster 1 contains customers with less recent purchases and lower average frequency and monetary value. These are descriptive profiles inferred from the results, not predefined labels.

## Dataset

The project uses the **Online Retail** dataset.

- **Primary source:** [UCI Machine Learning Repository — Online Retail](https://archive.ics.uci.edu/dataset/352/online%2Bretail)
- **Alternative download mirror:** [erkansirin78/datasets — OnlineRetail.csv](https://github.com/erkansirin78/datasets/blob/master/OnlineRetail.csv)

The dataset is not included in this repository. Download it to `project/data/raw/OnlineRetail.csv`:

```bash
mkdir -p project/data/raw
curl -L "https://raw.githubusercontent.com/erkansirin78/datasets/master/OnlineRetail.csv" -o project/data/raw/OnlineRetail.csv
```

Check the UCI page for attribution and licensing information before redistributing the dataset.

## Requirements

- Docker Engine
- Docker Compose
- A web browser

## Getting started

Clone the repository:

```bash
git clone git@github.com:moza369/ecommerce-customer-segmentation-big-data.git
cd ecommerce-customer-segmentation-big-data
```

Download the dataset using the command above, then start the services:

```bash
docker compose up -d
docker compose logs jupyter
```

Open [http://localhost:8888](http://localhost:8888). If authentication is requested, use the token printed in the Jupyter container logs.

The Compose configuration exposes:
- Jupyter: `localhost:8888`
- Spark UI: `localhost:4040` while a Spark application is running
- Kafka: `localhost:9092` from the host; use `kafka:9092` between containers on the Compose network

## Run the notebooks

Run the notebooks in this order.

### 1. Batch segmentation and algorithm comparison

Open `project/notebooks/01_customer_segmentation.ipynb` and run its cells in order.

The notebook performs data cleaning, RFM feature engineering, K-Means model selection, comparison with Agglomerative Clustering, customer-profile analysis, and saves the selected segmentation to:

`/home/jovyan/work/data/processed/customer_segments`

This Parquet output is required by the streaming notebook.

### 2. Kafka streaming

Open `project/notebooks/02_kafka_streaming_clean.ipynb` and run its cells in order. The notebook subscribes to the `customer-transactions` Kafka topic using `kafka:9092`.

The stream uses `startingOffsets=latest`. Start the streaming query before publishing test messages, or earlier messages may not be consumed by that query.

### 3. Create the Kafka topic

From the repository root, run:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --if-not-exists \
  --topic customer-transactions \
  --bootstrap-server kafka:9092 \
  --partitions 1 \
  --replication-factor 1
```

### 4. Publish a test transaction

The streaming notebook expects JSON with these fields and types:

- `CustomerID`: integer
- `InvoiceNo`: string
- `Quantity`: integer
- `UnitPrice`: number

After the streaming query is active, publish an example message:

```bash
printf '%s\n' '{"CustomerID":17850,"InvoiceNo":"DEMO-1001","Quantity":2,"UnitPrice":5.50}' \
  | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server kafka:9092 \
      --topic customer-transactions
```

Use a `CustomerID` present in the saved segmentation to see an assigned cluster. Unknown customers may have a null cluster. The streaming notebook uses a memory sink for demonstration, not durable storage.

## Repository structure

```text
.
├── .gitignore
├── README.md
├── docker-compose.yml
└── project/
    └── notebooks/
        ├── 01_customer_segmentation.ipynb
        └── 02_kafka_streaming_clean.ipynb
```

The raw dataset, generated Parquet files, and streaming checkpoints should remain excluded from version control.

## Notes and limitations

- The clustering comparison uses the same standardized RFM features, two clusters, and squared Euclidean silhouette distance.
- Agglomerative Clustering uses Ward linkage; K-Means is selected based on the higher measured silhouette score in this experiment.
- The streaming pipeline enriches transactions with an existing segmentation and does not retrain the model.
- The streaming query uses a memory sink and a local checkpoint directory for demonstration.
- A clean-machine setup has not yet been independently verified.

## Technologies

Python · Apache Spark · PySpark · Spark SQL · Spark MLlib · Apache Kafka · Structured Streaming · K-Means · Agglomerative Clustering · RFM Analysis · Docker Compose · Jupyter
