docker build -t faust_stream:1 -f Docker_file_faust_stream .
docker compose -f docker-compose.yaml up -d
docker compose -f docker-compose-faust.yaml up -d