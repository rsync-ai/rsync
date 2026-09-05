#!/bin/bash
# Quick reference for common e2e operations

case "$1" in
  start)
    docker compose -p rsync-ai-e2e -f docker-compose.e2e.dbs.yml up -d
    echo "✅ E2E databases started"
    echo "   MySQL:    localhost:3307"
    echo "   Postgres: localhost:5433"
    echo "   MinIO:    localhost:9000/9001"
    ;;
  stop)
    docker compose -p rsync-ai-e2e -f docker-compose.e2e.dbs.yml down
    echo "✅ E2E databases stopped"
    ;;
  clean)
    docker compose -p rsync-ai-e2e -f docker-compose.e2e.dbs.yml down -v
    echo "✅ E2E databases removed (including volumes)"
    ;;
  test)
    cd e2e && python3 test_pipeline_full.py
    ;;
  mysql)
    mysql -h 127.0.0.1 -P 3307 -u e2e_user -pe2e_password e2e_db
    ;;
  postgres)
    psql -h 127.0.0.1 -p 5433 -U e2e_user -d e2e_db
    ;;
  logs)
    docker compose -p rsync-ai-e2e -f docker-compose.e2e.dbs.yml logs -f
    ;;
  status)
    docker compose -p rsync-ai-e2e -f docker-compose.e2e.dbs.yml ps
    ;;
  *)
    echo "E2E Test Database Commands:"
    echo ""
    echo "  make e2e-start      Start e2e databases"
    echo "  make e2e-stop       Stop e2e databases"
    echo "  make e2e-clean      Remove e2e databases + volumes"
    echo "  make e2e-test       Run automated tests"
    echo "  make e2e-mysql      Connect to MySQL"
    echo "  make e2e-postgres   Connect to Postgres"
    echo "  make e2e-logs       View logs"
    echo "  make e2e-status     Show container status"
    echo ""
    echo "Manual commands:"
    echo "  docker compose -p rsync-ai-e2e -f docker-compose.e2e.dbs.yml up -d"
    echo "  cd e2e && python3 test_pipeline_full.py"
    ;;
esac

