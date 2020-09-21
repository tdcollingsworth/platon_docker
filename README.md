## Build Docker image
docker build --no-cache -t collingswortht/platon_app -f Dockerfile-platon-app .

## Run image
docker run --rm collingswortht/platon_app:latest --isolate 999888777
