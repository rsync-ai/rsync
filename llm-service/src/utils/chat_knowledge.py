"""
Local knowledge base for common chat questions.
This provides fast responses without calling OpenAI for simple queries.
"""

import re
from typing import Optional

class ChatKnowledge:
    """
    Knowledge base for handling common questions about RSYNC AI.
    """
    
    def __init__(self):
        self.knowledge = {
            "greeting": {
                "patterns": [
                    r"\b(hi|hello|hey|good morning|good afternoon|good evening)\b",
                    r"\bhow are you\b",
                ],
                "response": (
                    "Hello! I'm your RSYNC AI assistant. I'm here to help you with:\n\n"
                    "• Understanding pipeline status and execution\n"
                    "• Debugging pipeline failures\n"
                    "• Analyzing connection configurations\n"
                    "• Optimizing data flows\n"
                    "• Answering questions about transformations\n\n"
                    "How can I assist you today?"
                )
            },
            "what_is_rsync": {
                "patterns": [
                    r"\bwhat is rsync\b",
                    r"\bwhat.* rsync ai\b",
                    r"\btell me about rsync\b",
                ],
                "response": (
                    "RSYNC AI is an agentic data pipeline platform that provides:\n\n"
                    "• Automated data synchronization between sources and destinations\n"
                    "• Real-time Change Data Capture (CDC) for streaming data\n"
                    "• AI-powered pipeline planning and optimization\n"
                    "• Support for multiple connectors (MySQL, PostgreSQL, S3, etc.)\n"
                    "• Built-in monitoring and telemetry\n"
                    "• Intelligent error handling and recovery\n\n"
                    "Would you like to know more about a specific feature?"
                )
            },
            "capabilities": {
                "patterns": [
                    r"\bwhat can you (do|help)\b",
                    r"\byour capabilities\b",
                    r"\bhelp me with\b",
                ],
                "response": (
                    "I can help you with:\n\n"
                    "1. **Pipeline Management**: Create, monitor, and optimize data pipelines\n"
                    "2. **Debugging**: Identify and resolve pipeline failures\n"
                    "3. **Connections**: Configure and test source/destination connections\n"
                    "4. **CDC Pipelines**: Set up real-time data streaming with Debezium\n"
                    "5. **Performance**: Analyze and optimize pipeline performance\n"
                    "6. **Transformations**: Design data transformation logic\n"
                    "7. **Monitoring**: Interpret metrics and logs\n\n"
                    "What specific task would you like help with?"
                )
            },
            "connectors": {
                "patterns": [
                    r"\bwhat connectors\b",
                    r"\bsupported (sources|destinations)\b",
                    r"\blist.*connectors\b",
                ],
                "response": (
                    "RSYNC AI supports these connectors:\n\n"
                    "**Sources**:\n"
                    "• MySQL - Relational database\n"
                    "• PostgreSQL - Relational database\n"
                    "• Kafka - Message streaming\n"
                    "• Debezium - Change Data Capture\n\n"
                    "**Destinations**:\n"
                    "• S3/MinIO - Object storage\n"
                    "• Kafka - Message streaming\n"
                    "• PostgreSQL - Relational database\n\n"
                    "All connectors are implemented as MCP (Model Context Protocol) servers. "
                    "You can also generate custom connectors using our AI-powered tool generator!\n\n"
                    "Need help setting up a specific connector?"
                )
            },
            "cdc": {
                "patterns": [
                    r"\bwhat is cdc\b",
                    r"\bchange data capture\b",
                    r"\bhow.*cdc work\b",
                ],
                "response": (
                    "**Change Data Capture (CDC)** is a real-time data streaming technique:\n\n"
                    "**How it works**:\n"
                    "1. Monitors database transaction logs (binlog for MySQL)\n"
                    "2. Captures INSERT, UPDATE, DELETE operations in real-time\n"
                    "3. Streams changes to Kafka topics\n"
                    "4. Consumers process and store data (e.g., to S3)\n\n"
                    "**Benefits**:\n"
                    "• Near zero-latency data replication\n"
                    "• No impact on source database performance\n"
                    "• Complete change history\n"
                    "• Event-driven architecture support\n\n"
                    "**In RSYNC AI**:\n"
                    "We use Debezium connectors for CDC, with automatic schema evolution "
                    "and built-in error handling.\n\n"
                    "Would you like to set up a CDC pipeline?"
                )
            },
            "pipeline_status": {
                "patterns": [
                    r"\bpipeline status\b",
                    r"\bcheck.*pipeline\b",
                    r"\blist.*pipelines\b",
                ],
                "response": (
                    "To check pipeline status, you can:\n\n"
                    "1. **Via UI**: Navigate to the Pipelines page at http://localhost:3001/pipelines\n"
                    "2. **Via API**: GET /api/v1/pipelines\n"
                    "3. **Via Chat**: Tell me the pipeline ID and I can fetch its status\n\n"
                    "Common statuses:\n"
                    "• **running**: Pipeline is actively processing data\n"
                    "• **completed**: Batch pipeline finished successfully\n"
                    "• **failed**: Pipeline encountered an error\n"
                    "• **pending**: Waiting to start\n\n"
                    "Do you have a specific pipeline ID you'd like to check?"
                )
            },
        }
    
    def get_response(self, user_message: str) -> Optional[str]:
        """
        Check if the user message matches a known pattern and return local response.
        Returns None if no match found (should escalate to OpenAI).
        """
        message_lower = user_message.lower().strip()
        
        # Check each knowledge category
        for category, data in self.knowledge.items():
            for pattern in data["patterns"]:
                if re.search(pattern, message_lower, re.IGNORECASE):
                    return data["response"]
        
        # No match found - should use OpenAI
        return None
    
    def should_use_local(self, user_message: str) -> bool:
        """
        Determine if this question can be answered locally.
        """
        return self.get_response(user_message) is not None

