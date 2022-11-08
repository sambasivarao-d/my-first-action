# ms-azuretools.vscode-docker

terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "2.11.0"
    }
  }
}
provider "docker" {
  # Configuration options
  host = "npipe:////.//pipe//ms-azuretools.vscode-docker"
}

# Create a docker image resource
# -> docker pull nginx:latest
resource "docker_image" "nginx" {
  name         = "nginx:latest"
  keep_locally = true
}
