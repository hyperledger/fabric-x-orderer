## Building a multi-platform docker image

From project root:
```
cd images/multi-platform
```

### With podman

Create a manifest with the correct version (e.g. for git version v0.0.7 => 0.0.7):
```
podman manifest create icr.io/cbdc/arma-node:0.0.7
```

Generate a multi-platform image using podman:
```
podman build --platform linux/amd64,linux/s390x,linux/arm64 --manifest icr.io/cbdc/arma-node:0.0.7 -f Dockerfile ../../
```

### With docker buildx

Create a builder, make it default. It is better to name it and specify the target platform.
```
docker buildx create --name xplaform --platform linux/amd64,linux/s390x,linux/arm64 --use
```

Start the default builder (or rather, make sure it has booted):
```
docker buildx inspect --bootstrap
```

Generate an image for every platform and save the image to a tarball. The image is tagged by the "os-arch-version".
```
docker buildx build --platform linux/s390x --file Dockerfile --output type=docker,push=false,name=arma-node:linux-s390x-0.0.7,dest=arma-node_linux-s390x-0.0.7.tar ../../
```

```
docker buildx build --platform linux/amd64 --file Dockerfile --output type=docker,push=false,name=arma-node:linux-amd64-0.0.7,dest=arma-node_linux-amd64-0.0.7.tar ../../
```

```
docker buildx build --platform linux/arm64 --file Dockerfile --output type=docker,push=false,name=arma-node:linux-arm64-0.0.7,dest=arma-node_linux-arm64-0.0.7.tar ../../
```

One can then load said images to the local image store:
```
docker image load -i arma-node_linux-s390x-0.0.7.tar
```
```
docker image load -i arma-node_linux-amd64-0.0.7.tar
```
```
docker image load -i arma-node_linux-arm64-0.0.7.tar
```

Then re-tag them to ibm cloud container registry
```
docker image tag arma-node:linux-s390x-0.0.7 icr.io/cbdc/arma-node:linux-s390x-0.0.7
docker image tag arma-node:linux-amd64-0.0.7 icr.io/cbdc/arma-node:linux-amd64-0.0.7
docker image tag arma-node:linux-arm64-0.0.7 icr.io/cbdc/arma-node:linux-arm64-0.0.7
```

and push to icr.io (you and the docker agent need to be logged-in first)
```
docker image push icr.io/cbdc/arma-node:linux-s390x-0.0.7
docker image push icr.io/cbdc/arma-node:linux-amd64-0.0.7
docker image push icr.io/cbdc/arma-node:linux-arm64-0.0.7
```
--------------
Note: to login to IBM cloud:

ibmcloud login  --sso
ibmcloud target -g zrl-dec-trust-identity
ibmcloud cr login

--------------
Note: TODO automate in a script like:

for arch in amd64 arm64 s390x  ; do
docker buildx build \
  --platform $arch \
  --output "type=docker,push=false,name=me/myimage:mytag-$arch,dest=myimage.tar" \
  $path_to_dockerfile/
done