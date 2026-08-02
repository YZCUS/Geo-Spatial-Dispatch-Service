.PHONY: demo demo-up demo-down demo-logs demo-smoke test

demo:
	@echo "Demo UI: http://localhost:$${HTTP_PORT:-8080}"
	docker compose up --build

demo-up:
	docker compose up -d --build --wait
	@echo "Demo UI: http://localhost:$${HTTP_PORT:-8080}"

demo-down:
	docker compose down -v

demo-logs:
	docker compose logs -f app

demo-smoke:
	./scripts/demo-smoke.sh

test:
	docker compose up -d --wait redis
	go test -race ./... -count=1
