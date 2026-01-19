# ------------------------------------

# build the dummy pipeline image 
docker build -t test:testing .

# run the dummy pipeline container from the image
docker run -it --rm test:testing 12

# removing the container:
docker rm <container_id>

# Remove specific image
docker rmi test:testing

# ------------------------------------------------------

# List all containers
docker ps -a

# Remove all stopped containers
docker container prune

# ------------------------------------

# List all images
docker images

# Remove all unused images
docker image prune -a

# ------------------------------------

# List volumes
docker volume ls

# Remove specific volumes
docker volume rm ny_taxi_postgres_data
docker volume rm pgadmin_data

# Remove all unused volumes
docker volume prune

# ------------------------------------

# List networks
docker network ls

# Remove specific network
docker network rm pg-network

# Remove all unused networks
docker network prune

# ------------------------------------

# ⚠️ Warning: This removes ALL Docker resources!
docker system prune -a --volumes

# ------------------------------------

# Remove parquet files
rm *.parquet

# Remove Python cache
rm -rf __pycache__ .pytest_cache

# Remove virtual environment (if using venv)
rm -rf .venv

# ---------------------------------------------

docker run -it --rm \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v ny_taxi_postgres_data:/var/lib/postgresql \
  -p 5432:5432 \
  postgres:18

# ------------------------------------------------

uv run pgcli -h localhost -p 5432 -u root -d ny_taxi

# ----------------------------------------------------

# -- List tables
\dt

# -- Create a test table
CREATE TABLE test (id INTEGER, name VARCHAR(50));

# -- Insert data
INSERT INTO test VALUES (1, 'Hello Docker');

# -- Query data
SELECT * FROM test;

# -- Exit
\q

# ----------------------------------------------------

# Install Jupyter:

 uv add --dev jupyter

# Let's create a Jupyter notebook to explore the data:

 uv run jupyter notebook

# ----------------------------------------------------

uv run python ingest_data.py \
  --pg-user=root \
  --pg-pass=root \
  --pg-host=localhost \
  --pg-port=5432 \
  --pg-db=ny_taxi \
  --target-table=yellow_taxi_trips_2021_1 \
  --year=2021 \
  --month=1 \
  --chunksize=100000

# ----------------------------------------------------

