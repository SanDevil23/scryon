# Podman Setup (macOS)

## Install Podman

```bash
brew install podman
````

## Setup Podman Machine (required on macOS)

```bash
podman machine init
podman machine start
podman machine list
```

## Verify Installation

```bash
podman version
```

---

# Docker Compose Setup (with Podman)

Podman needs a compose provider to run `docker-compose.yml` files.

## Option 1: Podman Compose (recommended)

```bash
brew install podman-compose
```

Run:

```bash
podman-compose up
```

---

## Option 2: Podman native compose (if available)

```bash
podman compose up
```

---

## If compose fails

If you see:

> looking up compose provider failed

Install docker-compose:

```bash
brew install docker-compose
```

Then try again:

```bash
podman compose up
```
