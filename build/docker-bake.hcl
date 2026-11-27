# docker buildx bake -f build/docker-bake.hcl
#
# Bake exists here so local builds, CI builds and multi-arch release builds all
# run the same definition. A Makefile full of `docker build --build-arg ...`
# lines drifts from CI within a week.

variable "VERSION" { default = "dev" }
variable "COMMIT" { default = "unknown" }
variable "BUILD_DATE" { default = "unknown" }
variable "REGISTRY" { default = "ghcr.io/sharmasanskar" }
variable "IMAGE" { default = "ithaca" }

group "default" {
  targets = ["ithaca"]
}

target "_common" {
  context    = "."
  dockerfile = "build/ithaca.Dockerfile"
  args = {
    VERSION    = VERSION
    COMMIT     = COMMIT
    BUILD_DATE = BUILD_DATE
  }
  labels = {
    "org.opencontainers.image.title"       = "ithaca"
    "org.opencontainers.image.description" = "Webhook and event delivery platform"
    "org.opencontainers.image.source"      = "https://github.com/SharmaSanskar/ithaca"
    "org.opencontainers.image.version"     = VERSION
    "org.opencontainers.image.revision"    = COMMIT
    "org.opencontainers.image.created"     = BUILD_DATE
    "org.opencontainers.image.licenses"    = "MIT"
  }
}

# Local development: native arch only. Cross-compiling both architectures on a
# laptop roughly doubles the build for no benefit.
target "ithaca" {
  inherits = ["_common"]
  tags     = ["${IMAGE}:dev", "${IMAGE}:${VERSION}"]
  output   = ["type=docker"]
}

# k3d loads images from the local daemon, so this is the same as `ithaca` but
# named separately to keep the Makefile targets self-documenting.
target "ithaca-local" {
  inherits = ["_common"]
  tags     = ["${IMAGE}:local"]
  output   = ["type=docker"]
}

# CI release: multi-arch, pushed, with registry cache so an unchanged
# dependency tree does not get rebuilt on every commit.
target "ithaca-release" {
  inherits   = ["_common"]
  platforms  = ["linux/amd64", "linux/arm64"]
  # Only release tags (v*) build this target, so `latest` always means the
  # newest release, never a branch build.
  tags = [
    "${REGISTRY}/${IMAGE}:${VERSION}",
    "${REGISTRY}/${IMAGE}:${COMMIT}",
    "${REGISTRY}/${IMAGE}:latest",
  ]
  cache-from = ["type=registry,ref=${REGISTRY}/${IMAGE}:buildcache"]
  cache-to   = ["type=registry,ref=${REGISTRY}/${IMAGE}:buildcache,mode=max"]
  output     = ["type=registry"]
}
