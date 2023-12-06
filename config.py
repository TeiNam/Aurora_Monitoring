import os
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')

METRICS = [
    "AuroraBinlogReplicaLag",
    "ConnectionAttempts",
    "CPUUtilization",
    "DatabaseConnections",
    "Deadlocks",
    "DeleteThroughput",
    "DiskQueueDepth",
    "EngineUptime",
    "FreeableMemory",
    "InsertThroughput",
    "NetworkReceiveThroughput",
    "NetworkTransmitThroughput",
    "ReadIOPS",
    "ReadLatency",
    "ReadThroughput",
    "SelectThroughput",
    "SwapUsage",
    "UpdateThroughput",
    "WriteIOPS",
    "WriteLatency",
    "WriteThroughput",
    "Queries",
]