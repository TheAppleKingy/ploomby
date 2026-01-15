build.test:
	@docker compose up rabbitmq -d && sleep 3


tests: build.test
	@pytest -v tests/; docker compose down -v