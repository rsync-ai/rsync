#!/bin/bash
# Rsync AI - Development Start Script
# Combines: run_services.sh, start_all_services_go.sh, restart_services.sh

set -e

ACTION="${1:-start}"

case $ACTION in
  start)
    echo "🚀 Starting all Rsync AI services..."
    docker-compose up -d
    echo ""
    echo "⏳ Waiting for services to be ready..."
    sleep 5
    
    echo ""
    echo "✅ All services started!"
    echo ""
    echo "📍 Service URLs:"
    echo "  Frontend:     http://localhost:3000"
    echo "  API Gateway:  http://localhost:5001"
    echo "  Orchestrator: http://localhost:8081"
    echo "  LLM Service:  http://localhost:5011"
    echo ""
    echo "📊 Check status: docker-compose ps"
    echo "📝 View logs:    docker-compose logs -f [service]"
    ;;
    
  stop)
    echo "🛑 Stopping all services..."
    docker-compose down
    echo "✅ All services stopped"
    ;;
    
  restart)
    echo "🔄 Restarting all services..."
    docker-compose restart
    echo "✅ All services restarted"
    ;;
    
  rebuild)
    SERVICE="${2}"
    if [ -z "$SERVICE" ]; then
      echo "🏗️  Rebuilding all services..."
      docker-compose build --no-cache
      docker-compose up -d
    else
      echo "🏗️  Rebuilding $SERVICE..."
      docker-compose build --no-cache "$SERVICE"
      docker-compose up -d "$SERVICE"
    fi
    echo "✅ Rebuild complete"
    ;;
    
  logs)
    SERVICE="${2:-}"
    if [ -z "$SERVICE" ]; then
      docker-compose logs -f
    else
      docker-compose logs -f "$SERVICE"
    fi
    ;;
    
  ps)
    docker-compose ps
    ;;
    
  *)
    echo "Usage: $0 {start|stop|restart|rebuild [service]|logs [service]|ps}"
    echo ""
    echo "Examples:"
    echo "  $0 start              # Start all services"
    echo "  $0 stop               # Stop all services"
    echo "  $0 restart            # Restart all services"
    echo "  $0 rebuild frontend   # Rebuild just frontend"
    echo "  $0 logs api-gateway   # View API Gateway logs"
    echo "  $0 ps                 # Show service status"
    exit 1
    ;;
esac
