# example if it extracted /tmp/smartsheet_load/smartsheet-app-image.tar
docker load -i /tmp/smartsheet_load/*.tar


# Check what the image expects (ports, entrypoint, cmd):
docker inspect smartsheet-app:latest --format \
'Entrypoint={{json .Config.Entrypoint}} Cmd={{json .Config.Cmd}} ExposedPorts={{json .Config.ExposedPorts}}'


docker run -d --name smartsheet-app --restart unless-stopped -p 5000:5000 smartsheet-app:latest

docker ps
docker logs -n 100 smartsheet-app

curl -i http://127.0.0.1:5000/

docker start smartsheet-app
docker stop smartsheet-app
docker rm smartsheet-app