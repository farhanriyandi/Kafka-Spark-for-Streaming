# Kafka-Spark-for-Streaming
## How to run
1. Clone the repo
   ```
   git clone https://github.com/farhanriyandi/Kafka-Spark-for-Streaming.git
   cd Kafka-Spark-for-Streaming
   ```
   
2. Start the docker containers
   ```
   docker compose up -d
   ```
   You can open Kafka UI (web dashboard) at:  
    👉 [http://localhost:8080/](http://localhost:8080/)

3. Create 1 topics weather-data-flattened
    * Open a shell inside the Kafka container:
    ```bash
    docker exec -it riyandi_kafka bash
    ```
   **Note:** If you are using Linux and encounter permission issues, try:
   ```
   sudo chown -R 1001:1001 ./dockervol/kafka
   docker compose up -d
   ```
   Then repeat the step ```docker exec -it riyandi_kafka bash ``` above.

   * Create **weather-data-flattened** topic:
     ```bash
     kafka-topics.sh --create --topic weather-data-flattened --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
     ```
     
   * After that you can exit:
     ```
     exit
     ```

4. Create a table in PostgreSQL using the Docker container CLI
   * Run
   ```
   docker exec -it riyandi_postgres psql -U my-postgres -d my-postgres
   ```
   * Once inside the PostgreSQL CLI, execute the SQL commands provided in the file: **db_init/init.sql**
     
5. Create a virtual environment in another terminal **for Linux/macOS:**
   ```
    python3 -m venv .venv
   ```
   ```
   source .venv/bin/activate
   ```
   **Note:** You can use other methods to create and activate a virtual environment depending on your operating system or personal preference.

6. Install the dependencies
   ```
   pip install -r requirements.txt
   ```

7. Run the Python Scripts
   * Terminal 1 → Run the weather data generator + producer (to send data into kafka):
     ```
     python3 scripts/utils/post_to_kafka.py
     ```
   * Terminal 2 → Read from weather-data, flatten the JSON, then publish to weather-data-flattened:
     ```
     python3 scripts/utils/flatten_to_kafka.py
     ```
   * Terminal 3 → Run the script to aggregate the processed data and load it into PostgreSQL 
     ```
     python3 scripts/utils/aggregate_to_postgres.py
     ```

## Results

* **Kafka topic: `weather-data` (raw weather data before flattening)**
  <img width="820" height="344" alt="weather-data raw" src="https://github.com/user-attachments/assets/dc2168c4-328c-492f-9e9f-d04dc1d52134" />
  <img width="457" height="136" alt="weather-data raw sample" src="https://github.com/user-attachments/assets/eee8b0ab-4878-495e-b648-4eb8d58530a3" />

* **Kafka topic: `weather-data-flattened` (JSON after flattening)**
  <img width="793" height="257" alt="weather-data-flattened" src="https://github.com/user-attachments/assets/d615ace4-6bcf-41a7-b1c4-b0f53f335bd7" />
  <img width="449" height="95" alt="weather-data-flattened sample" src="https://github.com/user-attachments/assets/6bd952f1-10b8-4bd3-84ea-f24ee561d566" />

* **PostgreSQL table: `weather_data` (aggregated weather metrics)**
  <img width="582" height="400" alt="weather_data postgres" src="https://github.com/user-attachments/assets/b3ae69d5-f0a6-427c-be1d-d705556fef4a" />

  
  Then, in the PostgreSQL CLI (as shown in step 4), run the query:
  ```
  select * from weather_data;
  ```
   and the result would like to be in the below:
  <img width="731" height="429" alt="image" src="https://github.com/user-attachments/assets/6ed1439c-d950-42d6-9761-ee4715384bcf" />


✅ This confirms that the pipeline successfully ingests raw weather data, flattens it, and stores aggregated results into PostgreSQL.


   
