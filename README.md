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



